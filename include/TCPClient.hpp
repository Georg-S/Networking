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
		void readHeader(std::shared_ptr<Session> session);
		void readBody(std::shared_ptr<Session> session, std::shared_ptr<Message> unfinishedMessage);
		void sendMessage();
		void pushSystemMessage(net::SystemMessages messageType);
		void stopImpl();
		// True as long as an async operation started with the given connection ID still belongs to the current connection
		bool isCurrentConnectionID(uint64_t connectionID) const;

		std::string m_ip;
		std::thread m_thread;
		uint16_t m_port;
		boost::asio::io_context m_context;
		mutable std::mutex m_sessionMutex;
		// Synchronizes connectToServer() / stop() so that the io thread can only be joined once
		std::mutex m_lifecycleMutex;
		std::shared_ptr<Session> m_session;
		std::shared_ptr<tcp::socket> m_socket;
		uint64_t m_connectionID = 0;
		bool m_writeInFlight = false; // True while an async_write owns the queue front, so the front is written exactly once.
		ClientMessageQueue m_inMessages;
		ClientMessageQueue m_outMessages;
	};
}
