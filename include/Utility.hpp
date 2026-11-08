#pragma once
#include <memory>
#include <functional>
#include <thread>
#include <iostream>
#include <deque>
#include <boost/asio.hpp>
#include <boost/asio/ts/buffer.hpp>
#include <boost/asio/ts/internet.hpp>

#include "Message.hpp"
#include "ThreadSafeQueue.hpp"

namespace net 
{
	using boost::asio::ip::tcp;

	class Session
	{
	public:
		Session(std::shared_ptr<tcp::socket> socket);
		Session(std::shared_ptr<tcp::socket> socket, uint32_t id);
		virtual ~Session();
		void disconnect();
		bool isConnected() const;
		uint32_t id() const;

		std::shared_ptr<tcp::socket> m_socket;

	protected:
		uint32_t m_id;
	};

	// Simply extends the base Sesssion class with an authenticated flag
	class ServerSession : public Session
	{
	public:
		using Session::Session;
		bool isAuthenticated() const;
		void setAuthenticated(bool value);

	private:
		bool m_authenticated = false;
	};
}