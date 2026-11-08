set(CMAKE_CXX_STANDARD 17)
project(Networking)

set(HEADERS 
	"include/TCPServer.hpp"
	"include/TCPClient.hpp"
	"include/Message.hpp"
	"include/Utility.hpp"
	"include/ThreadSafeQueue.hpp"
	)

set(SOURCES 
	"src/TCPServer.cpp"
	"src/TCPClient.cpp"
	"src/Utility.cpp"
	)

if (MSVC)
	add_definitions(-D_WIN32_WINNT=0x0601)
endif (MSVC)

find_package(Boost REQUIRED) # Boost.Asio is header-only
add_library(Networking STATIC ${SOURCES} ${HEADERS})
set_target_properties(Networking PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(Networking PUBLIC "include" $ENV{BOOST_ROOT})
target_link_libraries(Networking PUBLIC Boost::headers)