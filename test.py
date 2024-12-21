import socket
import threading
import time

# Configuration
SEND_FROM_ADDRESS = ("127.0.0.1", 7000)        # Source address for sending
SEND_TO_ADDRESS = ("127.0.0.1", 5000)          # Rust socket bound address
RECEIVE_FROM_ADDRESS = ("127.0.0.1", 6000)     # Address to listen for forwarded packets

# Message to send
MESSAGE = "Hello, testing route from Python!"

# Reply message to send back
REPLY_MESSAGE = "Acknowledged: Message received!"

# Socket timeout in seconds
TIMEOUT = 2

def send_data(sender_socket, send_to_addr, message):
    """
    Send data from the sender_socket to the target address.

    Args:
        sender_socket (socket.socket): The socket used for sending data.
        send_to_addr (tuple): Tuple containing IP and port of the target receiver.
        message (str): The message to send.
    """
    try:
        # Send the message to the target address
        sender_socket.sendto(message.encode(), send_to_addr)
        print(f"Sent: '{message}' from {SEND_FROM_ADDRESS} to {send_to_addr}")
    except Exception as e:
        print(f"Error in send_data: {e}")

def receive_data(receive_from_addr, reply_message):
    """
    Receive data on the specified address and send a reply to the sender.

    Args:
        receive_from_addr (tuple): Tuple containing IP and port to bind the receiving socket.
        reply_message (str): The message to send back to the sender.
    """
    # Create a UDP socket
    receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        # Bind the socket to the specified address
        receiver_socket.bind(receive_from_addr)
        print(f"Receiver socket bound to {receive_from_addr}")

        # Set a timeout so the socket does not block indefinitely
        receiver_socket.settimeout(TIMEOUT)
        print(f"Receiver socket set to timeout after {TIMEOUT} seconds.\n")

        # Wait to receive data
        data, addr = receiver_socket.recvfrom(1024)  # Buffer size is 1024 bytes
        received_message = data.decode()
        print(f"Received: '{received_message}' from {addr}")

        # Prepare the reply
        reply = reply_message
        receiver_socket.sendto(reply.encode(), addr)
        print(f"Sent reply: '{reply}' to {addr}\n")

    except socket.timeout:
        print(f"No response received within {TIMEOUT} seconds.\n")
    except Exception as e:
        print(f"Error in receive_data: {e}\n")
    finally:
        # Close the socket
        receiver_socket.close()
        print("Receiver socket closed.\n")

def receive_reply(sender_socket):
    """
    Receive a reply on the sender_socket.

    Args:
        sender_socket (socket.socket): The socket used for sending data, also used for receiving replies.
    """
    try:
        # Set a timeout so the socket does not block indefinitely
        sender_socket.settimeout(TIMEOUT)
        print(f"Sender socket set to timeout after {TIMEOUT} seconds for receiving replies.\n")

        # Wait to receive a reply
        data, addr = sender_socket.recvfrom(1024)  # Buffer size is 1024 bytes
        received_reply = data.decode()
        print(f"Received reply: '{received_reply}' from {addr}\n")

    except socket.timeout:
        print(f"No reply received within {TIMEOUT} seconds.\n")
    except Exception as e:
        print(f"Error in receive_reply: {e}\n")

def receive_reply_thread(sender_socket):
    """
    Thread target for receiving replies.

    Args:
        sender_socket (socket.socket): The socket used for sending data, also used for receiving replies.
    """
    receive_reply(sender_socket)

if __name__ == "__main__":
    # Start a thread to listen for forwarded data and send replies
    receiver_thread = threading.Thread(
        target=receive_data,
        args=(RECEIVE_FROM_ADDRESS, REPLY_MESSAGE),
        daemon=True
    )
    receiver_thread.start()

    # Create a UDP socket for sending and receiving replies
    sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        # Bind the sender socket to the specified source address
        sender_socket.bind(SEND_FROM_ADDRESS)
        print(f"Sender socket bound to {SEND_FROM_ADDRESS}")

        # Start a thread to receive replies on the sender socket
        reply_thread = threading.Thread(
            target=receive_reply_thread,
            args=(sender_socket,),
            daemon=True
        )
        reply_thread.start()

        # Give the listener a moment to start
        time.sleep(1)

        # Send data to the Rust socket
        send_data(sender_socket, SEND_TO_ADDRESS, MESSAGE)

        # Wait for the reply thread to finish
        reply_thread.join()

    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        # Close the sender socket
        sender_socket.close()
        print("Sender socket closed.\n")

    # Wait for the receiver thread to finish
    receiver_thread.join()

    print("Python UDP communication completed.")
