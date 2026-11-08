#pragma once
#include <cstdint>
#include <memory>
#include <thread>
#include <iostream>
#include <deque>
#include <mutex>
#include <boost/asio.hpp>
#include <boost/asio/ts/buffer.hpp>
#include <boost/asio/ts/internet.hpp>

#include "ThreadSafeQueue.hpp"
#include "Utility.hpp"
#include "Message.hpp"

namespace net
{
	using ClientMessageQueue = ThreadSafeQueue<Message>;

	class TCPClient
	{
	public:
		TCPClient(std::string ipAddress, uint16_t port);
		TCPClient(const TCPClient&) = delete;
		TCPClient& operator=(const TCPClient&) = delete;
		virtual ~TCPClient();
		// Runs the TCP client in a new thread and connects to the server (asynchronous),
		// can be called again to reconnect when the connection was lost
		void connectToServer(); 
		void stop();
		size_t getMessageCount() const;
		std::shared_ptr<Message> getFirstMessage();
		std::shared_ptr<Message> getAndRemoveFirstMessage();
		void popFrontMessage();
		void sendMessage(std::shared_ptr<Message> message);
		bool isConnected() const;

	private:
		void connect();
		void connectTo(const tcp::resolver::results_type& endpoints);
		void disconnect();
		void readHeader();
		void readBody(std::shared_ptr<Message> unfinishedMessage);
		void sendMessage();
		void pushSystemMessage(net::SystemMessages messageType);

		std::string m_ip;
		std::thread m_thread;
		uint16_t m_port;
		boost::asio::io_context m_context;
		std::shared_ptr<Session> m_session;
		std::shared_ptr<tcp::socket> m_socket;
		ClientMessageQueue m_inMessages;
		ClientMessageQueue m_outMessages;
	};
}
