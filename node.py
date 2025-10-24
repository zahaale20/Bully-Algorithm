from enum import Enum
from datetime import date, datetime
import socket
import threading
import socketserver
import pickle
import time
import sys
import random

BUF_SZ = 1024
ELECTION_TIMEOUT = 3.0

class State(Enum):
	INIT = 1
	ELECTION_IN_PROGRESS = 2
	LEADER = 3
	FOLLOWER = 4
	WAITING_FOR_LEADER = 5

class Node:
	def __init__(self, gcd_host, gcd_port, month, day, su_id):
		# Process identity and GCD connection info
		self.process_id = (self.days_to_date(month, day), su_id)
		self.gcd_host = gcd_host
		self.gcd_port = gcd_port

		# Group membership and leader tracking
		self.members = {}
		self.leader = None
		self.state = State.INIT

		# TCP server info
		self.server = None
		self.host = None
		self.port = None

		# Thread-safe message queue
		self.messages = []
		self.message_sync = threading.Event()
		self.message_sync.set()
		self.message_flag = threading.Event()
		
		# Election timeout tracking
		self.election_timer = None
		self.election_lock = threading.RLock()
		self.got_response = False

	@staticmethod
	def days_to_date(month, day):
		today = date.today()
		this_year = today.year
		next_bday = date(this_year, month, day)
		if next_bday < today:
			next_bday = next_bday.replace(year=this_year + 1)
		return (next_bday - today).days

	def log(self, msg):
		print(f"[{datetime.now()}] [{self.process_id}] {msg}")

	def run(self):
		self.start_listener()
		self.join_group()
		self.start_election() # You initiate an election when you first join the group.
		while True:
			time.sleep(1)

	def start_listener(self):
		class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
			def handle(inner_self):
				try:
					data = pickle.loads(inner_self.request.recv(BUF_SZ))
					msg_type, msg_data = data
					self.handle_message(msg_type, msg_data, inner_self.request)
				except Exception as e:
					self.log(f"Error handling message: {e}")

		self.server = socketserver.ThreadingTCPServer(("localhost", 0), ThreadedTCPRequestHandler)
		self.host, self.port = self.server.server_address

		server_thread = threading.Thread(target=self.server.serve_forever)
		server_thread.daemon = True
		server_thread.start()
		print("Server loop running in thread:", server_thread.name)

	def handle_message(self, msg_type, msg_data, sock):
		if msg_type == "ELECT":
			sender_id, members_data = msg_data
			# When you receive an ELECT message, you update your membership list with any members you didn't already know about...
			for process_id, addr in members_data.items():
				if process_id not in self.members:
					self.members[process_id] = addr
					self.log(f"Learned about new member {process_id}")

			if self.state == State.LEADER:
				# ... then you respond with the text GOT_IT.
				sock.sendall(pickle.dumps(("GOT_IT", self.process_id)))
				self.log(f"Responded with GOT_IT to {sender_id} since I am the leader.")
				sender_addr = members_data.get(sender_id)
				if sender_addr:
					leader_info = {self.process_id: (self.host, self.port)}
					self.send_message(sender_addr, ("I_AM_LEADER", leader_info))
				return

			if self.state == State.ELECTION_IN_PROGRESS:
				# ... then you respond with the text GOT_IT.
				sock.sendall(pickle.dumps(("GOT_IT", self.process_id)))
				self.log(f"Already in election. Responded with GOT_IT to {sender_id}.")
				return

			# ... then you respond with the text GOT_IT.
			self.log(f"Received ELECT from {sender_id}, responded with GOT_IT, starting election.")
			# If you aren't in an election, then proceed as though you are initiating a new election.
			self.start_election()

		# There is no response to a I_AM_LEADER message.
		elif msg_type == "I_AM_LEADER":
			leader_process_id = list(msg_data.keys())[0]
			self.cancel_election_timeout()
			
			if self.leader is None or leader_process_id >= self.leader:
				if self.leader != leader_process_id:
					# If you receive an I_AM_LEADER message, note the (possibly) new leader.
					self.leader = leader_process_id
					# If you receive an I_AM_LEADER message, then change your state to not be election-in-progress.
					self.state = State.FOLLOWER
					self.members[leader_process_id] = msg_data[leader_process_id]
					self.log(f"Recognized {self.leader} as the new leader.")
					self.report_message((msg_type, msg_data))
					self.start_probing()
			else:
				self.log(f"Ignored I_AM_LEADER from lower-ID process {leader_process_id}.")

		elif msg_type == "GOT_IT":
			self.log(f"Received GOT_IT from {sender_id}.")
			with self.election_lock:
				self.got_response = True
				if self.state == State.ELECTION_IN_PROGRESS:
					self.start_election_timeout()
			self.report_message((msg_type, msg_data))

		elif msg_type == "PROBE":
			if self.state == State.LEADER:
				# The response to PROBE is the text GOT_IT.
				sock.sendall(pickle.dumps(("GOT_IT", None)))
				self.log(f"Received PROBE and responded with GOT_IT.")
		else:
			self.log(f"Received unknown message type: {msg_type}.")

	def join_group(self):
		# When starting up, contact the GCD and send a HOWDY message, which is a double: (process_id, listen_address).
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.connect((self.gcd_host, self.gcd_port))
			# The message content's process_id is the identity pair (days_to_moms_birthday, SU ID) and the listen_address is the pair (host, port) of your listening server.
			msg = ("HOWDY", (self.process_id, (self.host, self.port)))
			sock.sendall(pickle.dumps(msg))
			self.log("Sent HOWDY message to GCD.")
			# Response is a list of all the other members (some of which may now be failed).
			raw = sock.recv(BUF_SZ)
			try:
				self.members = pickle.loads(raw)
				if not isinstance(self.members, dict):
					raise ValueError("GCD response is not a dictionary")
				self.log(f"Received group members from GCD: {self.members}.")
			except Exception as e:
				self.log(f"Failed to parse GCD response: {e}. Using empty member list.")
				self.members = {}

	def start_election(self):
		with self.election_lock:
			self.log("Initiating election.")
			# While you are waiting for responses from higher processes, you put yourself into an election-in-progress state.
			self.state = State.ELECTION_IN_PROGRESS
			self.leader = None
			self.got_response = False
			self.cancel_election_timeout()
	
			# The ELECT message is sent to each member with a higher process id than your own.
			higher_nodes = {process_id: addr for process_id, addr in self.members.items() if process_id > self.process_id}
			# If you are the highest process id in the group, you win the election.
			if not higher_nodes:
				self.log("No higher process IDs. Declaring self as leader.")
				threading.Timer(0.5, self.declare_leader).start()
				return
			for process_id, address in higher_nodes.items():
				self.send_elect_message(process_id, address)
			
			# If none of the higher processes respond within the given time limit, you win the election.
			self.start_election_timeout()

	def send_elect_message(self, process_id, address):
		"""Send ELECT message in a non-blocking way"""
		def _send():
			try:
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
					sock.settimeout(2.0)
					sock.connect(address)
					sock.sendall(pickle.dumps(("ELECT", (self.process_id, self.members))))
					self.log(f"Sent ELECT to {process_id} at {address}.")
					
					try:
						response_raw = sock.recv(BUF_SZ)
						if response_raw:
							msg_type, msg_data = pickle.loads(response_raw)
							if msg_type == "GOT_IT":
								self.log(f"Received GOT_IT from {process_id}.")

								with self.election_lock:
									self.got_response = True
									if self.state == State.ELECTION_IN_PROGRESS:
										self.start_election_timeout()
								self.report_message((msg_type, msg_data))
					except socket.timeout:
						self.log(f"No immediate response from {process_id}.")
			except Exception as e:
				self.log(f"Failed to send ELECT to {process_id}: {e}")
		
		threading.Thread(target=_send, daemon=True).start()

	def start_election_timeout(self):
		"""Start/restart timeout for waiting for I_AM_LEADER"""
		self.cancel_election_timeout()
		self.election_timer = threading.Timer(ELECTION_TIMEOUT, self.election_timeout_handler)
		self.election_timer.daemon = True
		self.election_timer.start()
		self.log(f"Started election timeout ({ELECTION_TIMEOUT}s).")

	def cancel_election_timeout(self):
		"""Cancel any pending election timeout"""
		if self.election_timer:
			self.election_timer.cancel()
			self.election_timer = None

	def election_timeout_handler(self):
		"""Called when election timeout expires without receiving I_AM_LEADER"""
		with self.election_lock:
			if self.state == State.ELECTION_IN_PROGRESS:
				self.log("Election timeout: no I_AM_LEADER received. Declaring self as leader.")
				self._declare_leader_internal()

	def declare_leader(self):
		"""Method to declare leadership"""
		with self.election_lock:
			if self.state == State.ELECTION_IN_PROGRESS:
				self._declare_leader_internal()

	def _declare_leader_internal(self):
		"""Internal method - assumes lock is already held"""
		# The message data is the new leader's identity.
		self.state = State.LEADER
		self.leader = self.process_id
		self.cancel_election_timeout()
		self.log("I have won the election and am now the leader.")
		
		# If you win an election, you immediately send a I_AM_LEADER message to everyone.
		# The I_AM_LEADER message is sent by the group leader when she wins an election.
		leader_info = {self.process_id: (self.host, self.port)}
		for _, address in self.members.items():
			if address != (self.host, self.port):
				self.send_message(address, ("I_AM_LEADER", leader_info))

	def send_message(self, addr, message):
		def _send():
			try:
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
					sock.settimeout(2.0)
					sock.connect(addr)
					sock.sendall(pickle.dumps(message))
				self.log(f"Sent {message[0]} to {addr}.")
			except Exception as e:
				self.log(f"Failed to send {message[0]} to {addr}: {e}")
		threading.Thread(target=_send, daemon=True).start()

	def start_probing(self):
		if self.state != State.FOLLOWER:
			return
		
		def probe_loop():
			while self.state == State.FOLLOWER and self.leader:
				# The PROBE message is sent occasionally to the group leader by all the other members of the group.
				try:
					address = self.members.get(self.leader)
					if not address:
						self.log("Leader address unknown. Rejoining...")
						self.join_group()
						self.start_election()
						break

					with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
						sock.settimeout(1.0)
						sock.connect(address)
						# There is no message data.
						sock.sendall(pickle.dumps(("PROBE", None)))
						raw = sock.recv(BUF_SZ)
						msg_type, _ = pickle.loads(raw)
						if msg_type != "GOT_IT":
							raise Exception("Invalid probe response")

					self.log("Leader is alive (GOT_IT received).")

				except Exception as e:
					# You initiate an election when you notice the leader has failed.
					# A failed PROBE will trigger a new election, ...
					# ... but in this case re-register with the GCD first to get the latest peer list before starting the election.
					self.log(f"Leader probe failed: {e}. Starting new election...")
					self.join_group()
					self.start_election()
					break

				# Between each PROBE message, choose a random amount of time between 500 and 3000ms.
				time.sleep(random.uniform(0.5, 3.0))

		threading.Thread(target=probe_loop, daemon=True).start()

	def report_message(self, message):
		self.message_sync.wait()
		self.messages.append(message)
		self.message_flag.set()

	def harvest_messages(self):
		self.message_sync.clear()
		messages = self.messages
		self.messages = []
		self.message_flag.clear()
		self.message_sync.set()
		return messages

if __name__ == "__main__":
	if len(sys.argv) != 6:
		print("Usage: python3 node.py GCD_HOST GCD_PORT BIRTH_MONTH BIRTH_DAY SU_ID")
		sys.exit(1)

	gcd_host = sys.argv[1]
	gcd_port = int(sys.argv[2])
	month = int(sys.argv[3])
	day = int(sys.argv[4])
	su_id = int(sys.argv[5])

	node = Node(gcd_host, gcd_port, month, day, su_id)
	node.run()