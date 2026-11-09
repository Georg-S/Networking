#pragma once
#include <cstdint>
#include <vector>
#include <type_traits>
#include <cstddef>
#include <cassert>
#include <cstring>
#include <string>
#include <stdexcept>

namespace net
{
	static bool memcpy_secure(void* dest, size_t destSize, const void* source, size_t size)
	{
		if (!dest || !source || destSize < size)
			return false;

		std::memcpy(dest, source, size);
		return true;
	}

	enum DestinationID
	{
		SERVER = 0,
		BROADCAST = 1,
	};

	enum SystemMessages : uint32_t
	{
		// Has no body data and (only for the server) writes the id of the new connected client to the "fromID" and "toID"
		NEW_CONNECTION, 
		// Has no body data and (only for the server) writes the id of the lost connection client id to the "fromID" and "toID"
		CONNECTION_CLOSED,
		// Has no body data and (only for the client) gets sent when the client is unable to connect (e.g. due to the server being offline)
		UNABLE_TO_CONNECT,
		// Can have variable sized data (only for the server) gets passed through to the application layer which needs to confirm the authentication
		AUTHENTICATION,
		// End value of the system messages, to have a starting point for the custom messages of the application
		SYSTEM_MESSAGES_END
	};

	/// Utility class to help read and write complex objects from a message
	class DataBuffer 
	{
	public:
		DataBuffer() = default;
		DataBuffer(const DataBuffer&) = delete;
		DataBuffer& operator=(const DataBuffer&) = delete;
		DataBuffer(std::vector<std::byte> data) 
		{
			m_data = std::move(data);
			resetReading();
		}

		const std::vector<std::byte>& getData() const
		{
			return m_data;
		}

		void resetReading()
		{
			m_bytesRemaining = m_data.size();
			m_current = m_data.data();
		}

		template<typename T>
		void addDataToBuffer(const std::vector<T>& data)
		{
			static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
			static_assert(!std::is_pointer_v<T>, "Pointers are not allowed");

			uint64_t vectorSize = data.size();
			addDataToBuffer(vectorSize);
			addDataToBuffer(data.data(), data.size() * sizeof(T));
		}

		template<typename T>
		void addDataToBuffer(const T& data)
		{
			static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
			static_assert(!std::is_pointer_v<T>, "Pointers are not allowed");
			addDataToBuffer(&data, sizeof(data));
		}

		void addDataToBuffer(const void* data, size_t dataSize)
		{
			const size_t offset = m_data.size();
			m_data.resize(m_data.size() + dataSize);

			net::memcpy_secure(m_data.data() + offset, dataSize, data, dataSize);
			resetReading();
		}

		void addDataToBuffer(const std::string& str)
		{
			const uint64_t strSize = str.size();
			addDataToBuffer(strSize);
			addDataToBuffer(str.data(), strSize);
		}

		void addDataToBuffer(const std::vector<std::string>& data) 
		{
			uint64_t vectorSize = data.size();
			addDataToBuffer(vectorSize);
			for (const auto& elem : data)
				addDataToBuffer(elem);
		}

		template <typename T>
		void getDataFromBuffer(T* outData)
		{
			static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
			static_assert(!std::is_pointer_v<T>, "Pointers are not allowed");

			const size_t dataSize = sizeof(T);
			if (m_bytesRemaining < dataSize)
				throw std::runtime_error("Tried to read over buffer end");

			if (!outData || !memcpy_secure(outData, dataSize, m_current, dataSize))
				throw std::runtime_error("Tried to read over buffer end");

			m_bytesRemaining -= dataSize;
			m_current += dataSize;
		}

		template <typename T>
		void getDataFromBuffer(std::vector<T>* outData)
		{
			static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
			static_assert(!std::is_pointer_v<T>, "Pointers are not allowed");

			uint64_t vectorSize = 0;
			getDataFromBuffer(&vectorSize);
			if (!outData || vectorSize > m_bytesRemaining / sizeof(T))
				throw std::runtime_error("Tried to read over buffer end");

			const size_t dataBytecount = static_cast<size_t>(vectorSize) * sizeof(T);
			outData->assign(vectorSize, T{});
			if (!memcpy_secure(outData->data(), dataBytecount, m_current, dataBytecount))
				throw std::runtime_error("Tried to read over buffer end");

			m_bytesRemaining -= dataBytecount;
			m_current += dataBytecount;
		}

		void getDataFromBuffer(std::vector<std::string>* outData) 
		{
			uint64_t vectorSize = 0;
			getDataFromBuffer(&vectorSize);
			if (vectorSize > m_bytesRemaining / sizeof(uint64_t)) // sizeof(uint64_t) = Minimum string size (empty)
				throw std::runtime_error("Tried to read over buffer end");
			outData->resize(vectorSize);
			for (auto& elem : *outData) 
				getDataFromBuffer(&elem);
		}

		void getDataFromBuffer(std::string* outStr)
		{
			uint64_t stringSize = 0;
			getDataFromBuffer(&stringSize);
			if (stringSize > m_bytesRemaining) 
				throw std::runtime_error("Tried to read over buffer end");

			*outStr = std::string(reinterpret_cast<const char*>(m_current), stringSize);
			m_bytesRemaining -= stringSize;
			m_current += stringSize;
		}

	private:
		std::vector<std::byte> m_data;
		const std::byte* m_current = nullptr;
		size_t m_bytesRemaining = 0;
	};


	class Message
	{
	public:
		struct Header
		{
			uint32_t toID;
			uint32_t messageType;
			uint32_t bodySize;
		};
		Header header = {};
		std::vector<std::byte> dataBuffer;

		Message() = default; // Resize should be called once the header is set

		/// Simple message without any body
		template <typename Messagetype>
		Message(uint32_t toID, Messagetype messageType)
		{
			init(toID, messageType, nullptr, 0);
		}

		template <typename Messagetype, typename struc>
		Message(uint32_t toID, Messagetype messageType, const struc& data)
		{
			static_assert(std::is_trivially_copyable<struc>::value, "Must be trivially copyable");
			init(toID, messageType, &data, sizeof(data));
		}

		template <typename Messagetype>
		Message(uint32_t toID, Messagetype messageType, const void* data, uint32_t dataSize)
		{
			init(toID, messageType, data, dataSize);
		}

		template <typename Messagetype>
		Message(uint32_t toID, Messagetype messageType, const std::vector<std::byte>& data)
		{
			init(toID, messageType, static_cast<const void*>(data.data()), static_cast<uint32_t>(data.size()));
		}

		template <typename Messagetype>
		Message(uint32_t toID, Messagetype messageType, const std::string& dataStr) 
		{
			init(toID, messageType, dataStr.c_str(), dataStr.size() + 1);
		}

		template <typename Messagetype>
		Message(uint32_t toID, Messagetype messageType, const DataBuffer& dataBuffer)
		{
			init(toID, messageType, static_cast<const void*>(dataBuffer.getData().data()), static_cast<uint32_t>(dataBuffer.getData().size()));
		}

		Message(Header header)
			: header(std::move(header))
		{
			allocateAndInitializeBuffer();
		}

		// Resizes the buffer according to the headersize and the buffersize specified in the header
		// Zeroes out the complete body memory
		void resize() 
		{
			allocateAndInitializeBuffer();
		}

		void setToID(uint32_t toID) 
		{
			header.toID = toID;
			copyHeaderIntoDataBuffer();
		}

		void* getMessageStart() 
		{
			return dataBuffer.data();
		}

		const std::byte* getBodyStart() const
		{
			return dataBuffer.data() + headerSize();
		}

		std::byte* getBodyStart()
		{
			return dataBuffer.data() + headerSize();
		}

		DataBuffer getBodyAsDataBuffer() const
		{
			return DataBuffer(std::vector<std::byte>(getBodyStart(), getBodyStart() + bodySize()));
		}

		constexpr static uint32_t headerSize()
		{
			return sizeof(Header);
		}

		uint32_t bodySize() const
		{
			return static_cast<uint32_t>(header.bodySize);
		}

		uint32_t messageSize() const
		{
			return headerSize() + bodySize();
		}

	private:
		/// Initializes the message with the given data, only allows no data if the dataSize is zero
		template <typename Messagetype>
		void init(uint32_t toID, Messagetype messageType, const void* data, uint32_t dataSize)
		{
			static_assert(std::is_integral_v<Messagetype> || std::is_enum_v<Messagetype>, "Message type must be an integral or an enum");
			header.toID = toID;
			header.messageType = static_cast<uint32_t>(messageType);
			header.bodySize = dataSize;
			allocateAndInitializeBuffer();
			if (header.bodySize > 0)
				copyDataIntoBuffer(data, header.bodySize);
		}

		void allocateAndInitializeBuffer()
		{
			const auto bufferSize = size_t(header.bodySize) + headerSize();
			dataBuffer = std::vector<std::byte>(bufferSize, std::byte(0));
			copyHeaderIntoDataBuffer();
		}

		void copyHeaderIntoDataBuffer() 
		{
			memcpy_secure(dataBuffer.data(), headerSize(), static_cast<const void*>(&header), headerSize());
		}

		void copyDataIntoBuffer(const void* data, uint32_t dataSize)
		{
			memcpy_secure(getBodyStart(), header.bodySize, data, dataSize);
		}
	};

	class ServerMessage : public Message
	{
	public:
		uint32_t fromID;
	};
}
