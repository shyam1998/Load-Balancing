from socket import socket, AF_INET, SOCK_STREAM
from functools import partial
import threading
import multiprocessing
import queue
import select
import dill
import json
import csv
import random
import datetime
import math
import sys
import time


class Worker():
    def __init__(self):
        ### Metrics ###
        self.t1 = datetime.datetime.now()
        self.msg_count = 0
        self.csv_file = "metrics.csv"
        self.n_processes = "70"
        #################
        self.exit_flag = False
        self.id = 'w3'
        self.system_ip = "131.151.243.66"
        self.process_port = 9000
        self.msg_port = 9001
        self.result_port = 9002
        self.workers_id = ['w1', 'w2']
        self.servers_list = {"w1": ["131.151.243.64", 7000, 7001, 7002],
                             "w2": ["131.151.243.65", 8000, 8001, 8002]}
        self.threshold = 5
        self.misc_count = 8
        self.load = 0
        self.init_count = threading.active_count()
        self.numbers = list(range(4000000, 6000000, 100000))
        self.processes = queue.Queue()
        self.processes.queue = queue.deque(self.numbers)
        self.local_queue = queue.Queue(maxsize=5)
        self.server_queue = queue.Queue()
        self.server_results = queue.Queue()

    def process_listener(self):
        msg_server = socket(AF_INET, SOCK_STREAM)
        msg_server.setblocking(0)
        msg_server.bind((self.system_ip, self.process_port))
        msg_server.listen(5)
        print('Process listener started...')
        inputs = [msg_server]
        outputs = []
        while inputs:
            readable, writable, exceptional = select.select(inputs, outputs, inputs)
            for s in readable:
                if s is msg_server:
                    conn, addr = s.accept()
                    inputs.append(conn)
                else:
                    data = s.recv(4096)
                    if data:
                        self.process_handler(data, addr[0])
                    else:
                        inputs.remove(s)
                        s.close()

    def msg_listener(self):
        msg_server = socket(AF_INET, SOCK_STREAM)
        msg_server.setblocking(0)
        msg_server.bind((self.system_ip, self.msg_port))
        msg_server.listen(5)
        print('Message listener started...')
        inputs = [msg_server]
        outputs = []
        while inputs:
            readable, writable, exceptional = select.select(inputs, outputs, inputs)
            for s in readable:
                if s is msg_server:
                    conn, addr = s.accept()
                    inputs.append(conn)
                else:
                    data = s.recv(2048)
                    data = data.decode("utf-8")
                    if data:
                        self.msg_handler(data, addr[0])
                    else:
                        inputs.remove(s)
                        s.close()

    def result_listener(self):
        msg_server = socket(AF_INET, SOCK_STREAM)
        msg_server.setblocking(0)
        msg_server.bind((self.system_ip, self.result_port))
        msg_server.listen(5)
        print('Message listener started...')
        inputs = [msg_server]
        outputs = []
        while inputs:
            readable, writable, exceptional = select.select(inputs, outputs, inputs)
            for s in readable:
                if s is msg_server:
                    conn, addr = s.accept()
                    inputs.append(conn)
                else:
                    data = s.recv(2048)
                    data = data.decode("utf-8")
                    if data:
                        self.result_handler(data, addr[0])
                    else:
                        inputs.remove(s)
                        s.close()

    def process_handler(self, data, addr):
        idx = [i for i, v in enumerate(self.servers_list.values()) if str(addr) in v][0]
        wid = self.workers_id[idx]
        # print('Received process from:', wid)
        time_stamp = str(data).split('split_here')[1].strip("'")
        time_taken = datetime.datetime.now() - datetime.datetime.fromisoformat(time_stamp)
        time_taken = time_taken.seconds + (time_taken.microseconds * (10**-6))
        time_taken = format(time_taken, '.5f')
        with open(self.csv_file, 'a', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([self.n_processes, str(time_taken)])
        func = dill.loads(data)
        self.server_queue.put((wid, func))

    def msg_handler(self, data, addr):
        idx = [i for i, v in enumerate(self.servers_list.values()) if str(addr) in v][0]
        wid = self.workers_id[idx]
        if data == 'SMYP':
            if self.load >= self.threshold:
                self.send_process(worker_id=wid, p=self.processes.get())

    def result_handler(self, data, addr):
        idx = [i for i, v in enumerate(self.servers_list.values()) if str(addr) in v][0]
        wid = self.workers_id[idx]
        if "RESULT" in data:
            print(data)
            data = data.split(' ')[1]
            server_res_w3.put(int(data))
            # print(f"Result from {wid}:", data)

    def send_process(self, worker_id, p):
        func_text = dill.dumps(partial(sum_primes, p))
        dumps_file = func_text + b'split_here' + str(datetime.datetime.now()).encode()
        client = socket(AF_INET, SOCK_STREAM)
        client.setblocking(0)
        connected = False
        selected_server = self.servers_list[worker_id]
        while not connected:
            try:
                client.connect((selected_server[0], selected_server[1]))
                connected = True
            except:
                pass
        client.sendall(dumps_file)
        self.msg_count += 1
        client.close()

    def send_msg(self, worker_id, msg):
        client = socket(AF_INET, SOCK_STREAM)
        client.setblocking(0)
        connected = False
        selected_server = self.servers_list[worker_id]
        while not connected:
            try:
                client.connect((selected_server[0], selected_server[2]))
                connected = True
            except:
                pass
        client.sendall(bytes(msg, encoding="utf-8"))
        self.msg_count += 1
        client.close()

    def send_result(self, worker_id, msg):
        client = socket(AF_INET, SOCK_STREAM)
        client.setblocking(0)
        connected = False
        selected_server = self.servers_list[worker_id]
        while not connected:
            try:
                client.connect((selected_server[0], selected_server[3]))
                connected = True
            except:
                pass
        client.sendall(bytes(msg, encoding="utf-8"))
        self.msg_count += 1
        client.close()

    def result_sender(self):
        while True:
            if not self.server_results.qsize() == 0:
                wid, result = self.server_results.get()
                result = "RESULT" + ' ' + result
                self.send_result(worker_id=wid, msg=result)

    def load_tracker(self):
        while True:
            self.load = threading.active_count() - self.init_count - self.misc_count

    # def msg_count_tracker(self):
    #     time.sleep(5)
    #     while True:
    #         if self.load == 0:
    #             print("Messages Sent: ", self.msg_count)
    #             self.misc_count -= 1
    #             sys.exit()

    def request_process(self):
        while True:
            if self.load < self.threshold:
                worker = random.choice(self.workers_id)
                self.send_msg(worker, 'SMYP')
                time.sleep(1)

    def local(self):
        while True:
            if not self.processes.qsize() == 0:
                self.local_queue.put(self.processes.get())
                thread = threading.Thread(target=sum_primes, args=(self.local_queue.get(),))
                thread.start()
                if threading.active_count() >= self.threshold + self.init_count + self.misc_count:
                    thread.join()

    def server(self):
        while True:
            if not self.server_queue.qsize() == 0:
                wid, func = self.server_queue.get()
                wrapped_func = self.func_decorator(wid, func)
                thread = threading.Thread(target=wrapped_func)
                thread.start()

    def func_decorator(self, wid, func):
        def inner():
            self.server_results.put((wid, str(func())))
        return inner

    def start(self):
        time.sleep(2)
        while True:
            print('Load:', self.load, '\n')
            print('Local Results:', list(results_w3.queue), '\n')
            print('Server Results:', list(server_res_w3.queue), '\n')
            if self.load == 0 and len(list(results_w3.queue)) + len(list(server_res_w3.queue)) == len(self.numbers):
                time_taken = datetime.datetime.now() - self.t1
                print("Time elapsed:", time_taken)
                print("Messages Sent: ", self.msg_count)
                sys.exit()
            time.sleep(1)


results_w3 = queue.Queue()
server_res_w3 = queue.Queue()


def sum_primes(n):
    """Calculates sum of all primes below given integer n"""

    def isprime(x):
        """"pre-condition: n is a nonnegative integer
        post-condition: return True if n is prime and False otherwise."""
        if x < 2:
            return False
        if x % 2 == 0:
            return x == 2  # return False
        k = 3
        while k * k <= x:
            if x % k == 0:
                return False
            k += 2
        return True

    result = sum([x for x in range(2, n) if isprime(x)])
    try:
        results_w3.put(result)
    except:
        return result


if __name__ == "__main__":
    worker = Worker()
    p_listener = threading.Thread(target=worker.process_listener)
    msg_listener = threading.Thread(target=worker.msg_listener)
    result_listener = threading.Thread(target=worker.result_listener)
    load_tracker = threading.Thread(target=worker.load_tracker)
    # msg_count_tracker = threading.Thread(target=worker.msg_count_tracker)
    local = threading.Thread(target=worker.local)
    server = threading.Thread(target=worker.server)
    request_process = threading.Thread(target=worker.request_process)
    result_sender = threading.Thread(target=worker.result_sender)
    p_listener.start()
    msg_listener.start()
    result_listener.start()
    local.start()
    load_tracker.start()
    server.start()
    request_process.start()
    result_sender.start()
    # msg_count_tracker.start()
    worker.start()
