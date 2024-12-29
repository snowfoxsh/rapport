#!/usr/bin/env python3

import socket
import argparse
import logging
import time

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="UDP Receiver Script: Receives UDP packets and discards them."
    )
    parser.add_argument(
        '--listen-ip',
        type=str,
        default='127.0.0.1',
        help='IP address to listen on (default: 127.0.0.1)'
    )
    parser.add_argument(
        '--listen-port',
        type=int,
        default=6000,
        help='UDP port to listen on (default: 6000)'
    )
    parser.add_argument(
        '--packet-size',
        type=int,
        default=2048,
        help='Size of each UDP packet in bytes (default: 2048)'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=1,
        help='Socket timeout in seconds after last received packet (default: 2)'
    )
    return parser.parse_args()

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )

def udp_receiver(listen_ip, listen_port, packet_size, timeout):
    """
    Receives UDP packets on the specified IP and port, discards them, and logs progress.
    The timeout starts after the first packet is received.
    """
    # Create UDP socket
    receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver_socket.bind((listen_ip, listen_port))

    # Initially, set socket to blocking mode with no timeout
    receiver_socket.settimeout(None)
    logging.info(f"Receiver socket bound to {listen_ip}:{listen_port}")
    logging.info(f"Initial socket timeout: None (blocking mode)")
    logging.info("Receiver is ready to receive data...\n")

    total_packets = 0
    start_time = None  # To be set after first packet
    last_packet_time = None  # To track last received packet time
    first_packet_received = False  # Flag to check if first packet is received

    try:
        while True:
            try:
                data, addr = receiver_socket.recvfrom(packet_size)
                total_packets += 1
                current_time = time.time()

                if not first_packet_received:
                    # First packet received; start the timeout countdown
                    first_packet_received = True
                    start_time = current_time
                    last_packet_time = current_time
                    # Set the socket timeout
                    receiver_socket.settimeout(timeout)
                    logging.info(f"First packet received from {addr}.")
                    logging.info(f"Socket timeout set to {timeout} seconds.\n")
                else:
                    # Update the time of the last received packet
                    last_packet_time = current_time

                # Log progress every 100,000 packets
                if total_packets % 100000 == 0:
                    elapsed = last_packet_time - start_time
                    logging.info(f"Received {total_packets} packets in {elapsed:.2f} seconds.")

            except socket.timeout:
                if first_packet_received:
                    logging.info(f"No packets received for {timeout} seconds. Stopping receiver.")
                    break
                else:
                    # This block shouldn't be reached because timeout is None before first packet
                    logging.info(f"No packets received yet. Continuing to wait.")

            except Exception as e:
                logging.error(f"Error receiving data: {e}")
                break

    finally:
        receiver_socket.close()
        logging.info("Receiver socket closed.\n")
        total_time = time.time() - start_time if start_time else 0
        logging.info(f"Total packets received: {total_packets}")
        logging.info(f"Total time: {total_time:.2f} seconds.")
        if total_time > 0:
            logging.info(f"Average throughput: {total_packets / total_time:.2f} packets/second.")

def main():
    args = parse_arguments()
    setup_logging()
    udp_receiver(args.listen_ip, args.listen_port, args.packet_size, args.timeout)

if __name__ == "__main__":
    main()
