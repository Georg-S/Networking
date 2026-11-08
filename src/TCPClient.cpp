#include "TCPClient.hpp"

using namespace net;

net::TCPClient::TCPClient(std::string ipAddress, uint16_t port)
	: m_ip(std::move(ipAddress))
	, m_port(port)
{
}

net::TCPClient::~TCPClient()
{
	stop();
}

void net::TCPClient::connectToServer()
{
	std::scoped_lock lock(m_lifecycleMutex);
	stopImpl();
	connect();
	m_context.restart();
	m_thread = std::thread([this]() {m_context.run(); });
}

void net::TCPClient::stop()
{
	std::scoped_lock lock(m_lifecycleMutex);
	stopImpl();
}

void net::TCPClient::stopImpl()
{
	m_context.stop();
	if (m_thread.joinable())
		m_thread.join();
	++m_connectionID;
	m_writeInFlight = false;
	disconnect();
	m_outMessages.clear();
	std::cout << "Client stopped" << std::endl;
}

bool net::TCPClient::isCurrentConnectionID(uint64_t connectionID) const
{
	// Safe unsynchronized read: the only writer is stopImpl(), which runs while the io thread is not executing
	return connectionID == m_connectionID;
}

size_t net::TCPClient::getMessageCount() const
{
	return m_inMessages.size();
}

std::shared_ptr<Message> net::TCPClient::getFirstMessage()
{
	return m_inMessages.front();
}

std::shared_ptr<Message> net::TCPClient::getAndRemoveFirstMessage()
{
	return m_inMessages.getAndRemoveFirstElement();
}

void net::TCPClient::popFrontMessage()
{
	m_inMessages.popFront();
}

void net::TCPClient::sendMessage(std::shared_ptr<Message> message)
{
	auto size = m_outMessages.pushBack(message);
	if (size == 1)
		boost::asio::post(m_context, [this]() {sendMessage(); });
}

bool net::TCPClient::isConnected() const
{
	std::scoped_lock lock(m_sessionMutex);
	return m_session && m_session->isConnected();
}

void net::TCPClient::connect()
{
	boost::system::error_code ec;
	auto socket = std::make_shared<tcp::socket>(m_context);
	{
		std::scoped_lock lock(m_sessionMutex);
		m_socket = socket;
	}
	auto resolver = tcp::resolver(m_context);
	auto endPoints = resolver.resolve(m_ip, std::to_string(m_port), ec);
	if (ec)
		throw std::runtime_error("Unable to resolve ip(" + m_ip + ") error: " + std::string(ec.message()));

	connectTo(endPoints);
}

void net::TCPClient::connectTo(const tcp::resolver::results_type& endpoints)
{
	std::shared_ptr<tcp::socket> socket;
	{
		std::scoped_lock lock(m_sessionMutex);
		socket = m_socket;
	}
	if (!socket)
		return;

	const auto connectionID = m_connectionID;
	boost::asio::async_connect(*socket, endpoints,
		[this, socket, connectionID](boost::system::error_code ec, tcp::endpoint tcpEndpoint) mutable
		{
			if (!isCurrentConnectionID(connectionID))
				return; // Stale completion from a previous connection
			if (ec)
			{
				pushSystemMessage(UNABLE_TO_CONNECT);
				std::cout << "Connecting to server failed: " << std::endl << ec.message() << std::endl;
				return;
			}

			std::cout << "Connected to server" << std::endl;
			boost::system::error_code keepaliveEc;
			socket->set_option(boost::asio::socket_base::keep_alive(true), keepaliveEc);
			pushSystemMessage(NEW_CONNECTION);
			auto session = std::make_shared<Session>(socket);
			{
				std::scoped_lock lock(m_sessionMutex);
				m_session = session;
			}
			readHeader(session);
		});
}

void net::TCPClient::disconnect()
{
	std::scoped_lock lock(m_sessionMutex);
	if (m_session && m_session->isConnected())
	{
		m_session->disconnect(); // For now if we encounter an error while writing / reading a message ... disconnect
		pushSystemMessage(CONNECTION_CLOSED);
	}
	m_session = nullptr;
	m_socket = nullptr;
}

void net::TCPClient::readHeader(std::shared_ptr<Session> session)
{
	auto currentMessage = std::make_shared<Message>();
	const uint64_t connectionID = m_connectionID;
	boost::asio::async_read(*session->m_socket, boost::asio::buffer(&currentMessage->header, Message::headerSize()),
		[this, session, currentMessage, connectionID](boost::system::error_code ec, size_t bytesRead)
		{
			if (!isCurrentConnectionID(connectionID))
				return; // Stale completion from a previous connection
			if (ec)
			{
				std::cout << "Error reading message header ... closing session: " << std::endl << ec.message() << std::endl;
				disconnect();
				return;
			}
			currentMessage->resize();

			readBody(session, currentMessage);
		}
	);
}

void net::TCPClient::readBody(std::shared_ptr<Session> session, std::shared_ptr<Message> unfinishedMessage)
{
	const uint64_t connectionID = m_connectionID;
	boost::asio::async_read(*session->m_socket, boost::asio::buffer(unfinishedMessage->getBodyStart(), unfinishedMessage->bodySize()),
		[this, session, unfinishedMessage, connectionID](boost::system::error_code ec, size_t bytesRead)
		{
			if (!isCurrentConnectionID(connectionID))
				return; // Stale completion from a previous connection
			if (ec)
			{
				std::cout << "Error reading message body ... closing session: " << std::endl << ec.message() << std::endl;
				disconnect();
				return;
			}
			m_inMessages.pushBack(unfinishedMessage);
			readHeader(session);
		});
}

void net::TCPClient::sendMessage()
{
	auto message = m_outMessages.front();

	std::shared_ptr<tcp::socket> socket;
	uint64_t connectionID = 0;
	{
		std::scoped_lock lock(m_sessionMutex);
		if (m_writeInFlight)
			return; // The queue front is already being written, its completion handler continues the queue
		socket = m_socket;
		connectionID = m_connectionID;
		if (socket && message)
			m_writeInFlight = true;
	}
	if (!socket)
	{
		m_outMessages.clear();
		return;
	}
	if (!message)
		return; // Can happen when a stale posted invocation runs after stopImpl() cleared the queue

	boost::asio::async_write(*socket, boost::asio::buffer(message->getMessageStart(), message->messageSize()),
		[this, socket, message, connectionID](boost::system::error_code ec, std::size_t bytesWritten)
		{
			if (!isCurrentConnectionID(connectionID))
				return; // Stale completion from a previous connection, stopImpl() already cleared the queue
			m_writeInFlight = false;

			const auto newSize = m_outMessages.popFront();
			if (ec)
			{
				std::cout << "Error occured while writing message ... closing socket: " << std::endl << ec.message() << std::endl;
				disconnect();
				if (newSize > 0)
					boost::asio::post(m_context, [this]() {sendMessage(); });
				return;
			}
			//std::cout << "Sent " << bytesWritten << " bytes." << std::endl;

			if (newSize > 0)
				sendMessage();
		});
}

void net::TCPClient::pushSystemMessage(net::SystemMessages messageType)
{
	auto disconnectMessage = std::make_shared<ServerMessage>();
	disconnectMessage->header.messageType = messageType;
	m_inMessages.pushBack(disconnectMessage);
}
