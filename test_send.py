#!/usr/bin/env python3

import socket
import argparse
import logging
import time

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="UDP Sender Script: Sends UDP packets from a specific source port to a target address."
    )
    parser.add_argument(
        '--source-ip',
        type=str,
        default='127.0.0.1',
        help='Source IP address to bind to (default: 127.0.0.1)'
    )
    parser.add_argument(
        '--source-port',
        type=int,
        default=7000,
        help='Source UDP port to bind to (default: 7000)'
    )
    parser.add_argument(
        '--dest-ip',
        type=str,
        default='127.0.0.1',
        help='Destination IP address to send to (default: 127.0.0.1)'
    )
    parser.add_argument(
        '--dest-port',
        type=int,
        default=6000,
        help='Destination UDP port to send to (default: 6000)'
    )
    parser.add_argument(
        '--packet-size',
        type=int,
        default=2048,
        help='Size of each UDP packet in bytes (default: 2048)'
    )
    parser.add_argument(
        '--packet-count',
        type=int,
        default=512000,
        help='Number of UDP packets to send (default: 512000)'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=2,
        help='Socket timeout in seconds for receiving replies (default: 2)'
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

def udp_sender(source_ip, source_port, dest_ip, dest_port, packet_size, packet_count, timeout):
    """
    Sends UDP packets filled with zero bytes from the specified source to the destination.
    """
    # Create UDP socket
    sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sender_socket.bind((source_ip, source_port))
    sender_socket.settimeout(timeout)
    logging.info(f"Sender socket bound to {source_ip}:{source_port}")
    logging.info(f"Sending UDP packets to {dest_ip}:{dest_port}")
    logging.info(f"Packet size: {packet_size} bytes, Packet count: {packet_count}\n")

    packet_data = b'\0' * packet_size  # Data to send in each packet

    start_time = time.time()

    try:
        for i in range(1, packet_count + 1):
            try:
                sender_socket.sendto(packet_data, (dest_ip, dest_port))
                if i % 100000 == 0:
                    elapsed = time.time() - start_time
                    logging.info(f"Sent {i} packets in {elapsed:.2f} seconds.")
            except Exception as e:
                logging.error(f"Error sending packet {i}: {e}")
                break
    finally:
        logging.info("\nFinished sending packets.")
        total_time = time.time() - start_time
        logging.info(f"Total time: {total_time:.2f} seconds.")
        if total_time > 0:
            logging.info(f"Average throughput: {packet_count / total_time:.2f} packets/second.\n")

    # Optionally, wait for a reply (if applicable)
    try:
        logging.info("Waiting for acknowledgment reply...")
        data, addr = sender_socket.recvfrom(4096)  # Buffer size 4 KB
        received_reply = data.decode()
        logging.info(f"Received reply: '{received_reply}' from {addr}\n")
    except socket.timeout:
        logging.warning(f"No reply received within {timeout} seconds.\n")
    except Exception as e:
        logging.error(f"Error receiving reply: {e}\n")
    finally:
        sender_socket.close()
        logging.info("Sender socket closed.\n")

def main():
    args = parse_arguments()
    setup_logging()
    udp_sender(
        args.source_ip,
        args.source_port,
        args.dest_ip,
        args.dest_port,
        args.packet_size,
        args.packet_count,
        args.timeout
    )

if __name__ == "__main__":
    main()
