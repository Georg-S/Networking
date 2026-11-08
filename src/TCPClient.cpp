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
	stop();
	connect();
	m_context.restart();
	m_thread = std::thread([this]() {m_context.run(); });
}

void net::TCPClient::stop()
{
	m_context.stop();
	if (m_thread.joinable())
		m_thread.join();
	disconnect();
	m_socket = nullptr;
	std::cout << "Client stopped" << std::endl;
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
	if (!m_session)
		return false;

	return m_session->isConnected();
}

void net::TCPClient::connect()
{
	boost::system::error_code ec;
	m_socket = std::make_shared<tcp::socket>(m_context);
	auto resolver = tcp::resolver(m_context);
	auto endPoints = resolver.resolve(m_ip, std::to_string(m_port), ec);
	if (ec) 
		throw std::runtime_error("Unable to resolve ip(" + m_ip + ") error: " + std::string(ec.message()));

	connectTo(endPoints);
}

void net::TCPClient::connectTo(const tcp::resolver::results_type& endpoints)
{
	boost::asio::async_connect(*m_socket, endpoints,
		[this](boost::system::error_code ec, tcp::endpoint tcpEndpoint) mutable
		{
			if (ec)
			{
				pushSystemMessage(UNABLE_TO_CONNECT);
				std::cout << "Connecting to server failed: " << std::endl << ec.message() << std::endl;
				return;
			}

			std::cout << "Connected to server" << std::endl;
			boost::system::error_code keepaliveEc;
			m_socket->set_option(boost::asio::socket_base::keep_alive(true), keepaliveEc);
			pushSystemMessage(NEW_CONNECTION);
			m_session = std::make_shared<Session>(m_socket);
			readHeader();
		});
}

void net::TCPClient::disconnect()
{
	if (isConnected())
	{
		m_session->disconnect(); // For now if we encounter an error while writing / reading a message ... disconnect
		pushSystemMessage(CONNECTION_CLOSED);
		m_session = nullptr;
	}
}

void net::TCPClient::readHeader()
{
	auto currentMessage = std::make_shared<Message>();
	boost::asio::async_read(*m_session->m_socket, boost::asio::buffer(&currentMessage->header, Message::headerSize()),
		[this, currentMessage](boost::system::error_code ec, size_t bytesRead)
		{
			if (ec)
			{
				std::cout << "Error reading message header ... closing session: " << std::endl << ec.message() << std::endl;
				disconnect();
				return;
			}
			currentMessage->resize();

			readBody(currentMessage);
		}
	);
}

void net::TCPClient::readBody(std::shared_ptr<Message> unfinishedMessage)
{
	boost::asio::async_read(*m_session->m_socket, boost::asio::buffer(unfinishedMessage->getBodyStart(), unfinishedMessage->bodySize()),
		[this, unfinishedMessage](boost::system::error_code ec, size_t bytesRead)
		{
			if (ec)
			{
				std::cout << "Error reading message body ... closing session: " << std::endl << ec.message() << std::endl;
				disconnect();
				return;
			}
			m_inMessages.pushBack(unfinishedMessage);
			readHeader();
		});
}

void net::TCPClient::sendMessage()
{
	assert(!m_outMessages.empty());
	auto message = m_outMessages.front();

	boost::asio::async_write(*m_socket, boost::asio::buffer(message->getMessageStart(), message->messageSize()),
		[this](boost::system::error_code ec, std::size_t bytesWritten)
		{
			if (ec)
			{
				std::cout << "Error occured while writing message ... closing socket: " << std::endl << ec.message() << std::endl;
				disconnect();
				return;
			}
			//std::cout << "Sent " << bytesWritten << " bytes." << std::endl;

			auto newSize = m_outMessages.popFront();
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
