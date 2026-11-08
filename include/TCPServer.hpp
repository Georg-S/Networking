#pragma once
#include <optional>
#include <memory>
#include <map>
#include <mutex>
#include <thread>
#include <boost/asio.hpp>
#include <boost/asio/ts/buffer.hpp>
#include <boost/asio/ts/internet.hpp>

#include "Message.hpp"
#include "Utility.hpp"

namespace net
{
	using boost::asio::ip::tcp;

	class TCPServer
	{
	public:
		TCPServer(std::string ip, uint16_t port, uint32_t authenticationMessageMaxSize);
		TCPServer(const TCPServer&) = delete;
		TCPServer& operator=(const TCPServer&) = delete;
		virtual ~TCPServer();
		void start();
		void stop();
		/// Set the maximum allowed connections / client, nullopt = no limit
		void setMaxAllowedConnections(std::optional<uint32_t> maxAllowedConnections);
		size_t getCountOfConnectedClients() const;
		std::shared_ptr<ServerMessage> getAndRemoveFirstMessage();
		void sendMessage(std::shared_ptr<Message> message);
		void broadcastMessage(std::shared_ptr<Message> message, std::initializer_list<uint32_t> ignoreIDs = {});
		void disconnectClient(uint32_t id);
		bool isAuthenticated(uint32_t id) const;
		void setAuthenticated(uint32_t id, bool value);

	private:
		void sendMessage();
		void sendMessage(std::shared_ptr<Message> message, std::shared_ptr<ServerSession> session);
		void acceptConnection();
		void readHeader(std::shared_ptr<ServerSession> session);
		void readBody(std::shared_ptr<ServerSession> session, std::shared_ptr<ServerMessage> unfinishedMessage);
		void disconnectSession(std::shared_ptr<ServerSession> session);

		static constexpr uint32_t SessionStartId = 2;
		mutable std::mutex m_sessionMutex;
		// Synchronizes start() / stop() so that the io thread can only be joined once
		std::mutex m_lifecycleMutex;
		boost::asio::io_context m_context;
		std::map<uint32_t, std::shared_ptr<ServerSession>> m_sessions;
		std::unique_ptr<tcp::acceptor> m_acceptor;
		std::thread m_thread;
		uint32_t m_sessionId = SessionStartId;
		/*
		If the client is actually connected to the internet,
		there will be random port sniffers trying to connect to the server.
		Therefore we need to move a part of the authentication into this lib 
		and only authentication messages are allowed until the client is authenticated
		*/
		uint32_t m_authenticationMessageMaxBodySize = 1000000; 
		std::optional<uint32_t> m_maxAllowedConnections;
		ThreadSafeQueue<ServerMessage> m_inMessages;
		ThreadSafeQueue<Message> m_outMessages;
	};
}
