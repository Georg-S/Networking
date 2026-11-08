#pragma once
#include <deque>


template<typename T>
class ThreadSafeQueue
{
public:
	ThreadSafeQueue() = default;

	size_t size() const 
	{
		std::scoped_lock lock(m_mut);
		return m_elements.size();
	};

	/// Returns the size after adding the element
	size_t pushBack(std::shared_ptr<T> element) 
	{
		std::scoped_lock lock(m_mut);
		m_elements.emplace_back(std::move(element));
		return m_elements.size();
	}

	std::shared_ptr<T> front()
	{
		std::scoped_lock lock(m_mut);

		if (m_elements.empty())
			return nullptr;
		return m_elements.front();
	}

	// Return nullptr if empty
	std::shared_ptr<T> getAndRemoveFirstElement() 
	{
		std::scoped_lock lock(m_mut);
		if (m_elements.empty())
			return nullptr;

		auto element = m_elements.front();
		m_elements.pop_front();

		return element;
	}

	/// Returns the size after removing the element
	size_t popFront() 
	{
		std::scoped_lock lock(m_mut);
		if (!m_elements.empty())
			m_elements.pop_front();

		return m_elements.size();
	}

	bool empty() const 
	{
		std::scoped_lock lock(m_mut);
		return m_elements.empty();
	}

private:
	mutable std::mutex m_mut;
	std::deque<std::shared_ptr<T>> m_elements;
};