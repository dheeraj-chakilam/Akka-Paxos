#!/usr/bin/env python
"""
The master program for CS5414 Paxos project.
"""

import os
import signal
import subprocess
import sys
import time
from socket import SOCK_STREAM, socket, AF_INET
from threading import Thread, Lock

address = 'localhost'
threads = {}

msgs = {}

ack_lock = Lock()
acked_list = {}
wait_for_ack_list = {}
wait_for_ack = False
wait_chat_log = False

started_processes = {}

debug = False


class ClientHandler(Thread):
    def __init__(self, index, address, port, process):
        Thread.__init__(self)
        self.daemon = True
        self.index = index
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect((address, port))
        self.buffer = ""
        self.valid = True
        self.process = process

    def run(self):
        global threads, wait_chat_log, wait_for_ack
        while self.valid:
            if "\n" in self.buffer:
                (l, rest) = self.buffer.split("\n", 1)
                self.buffer = rest
                s = l.split()
                if len(s) < 2:
                    continue
                if s[0] == 'ack':
                    mid = int(s[1])
                    ack_lock.acquire()
                    acked_list[mid] = True
                    if mid in wait_for_ack_list:
                        del wait_for_ack_list[mid]
                    if len(wait_for_ack_list) == 0:
                        wait_for_ack = False
                    ack_lock.release()
                elif s[0] == 'chatLog':
                    chatLog = s[1]
                    print chatLog
                    wait_chat_log = False
                else:
                    print 'WRONG MESSAGE:', s
            else:
                try:
                    data = self.sock.recv(1024)
                    # sys.stderr.write(data)
                    self.buffer += data
                except:
                    # print sys.exc_info()
                    self.valid = False
                    del threads[self.index]
                    self.sock.close()
                    break

    def kill(self):
        if self.valid:
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
            except:
                pass
            self.close()

    def send(self, s):
        if self.valid:
            self.sock.send(str(s) + '\n')

    def close(self):
        try:
            self.valid = False
            self.sock.close()
        except:
            pass


def send(index, data, set_wait=False):
    global threads, wait_chat_log
    while wait_chat_log:
        time.sleep(0.01)
    pid = int(index)
    if set_wait:
        wait_chat_log = True
    threads[pid].send(data)


def exit(is_exit=False):
    global threads, wait_chat_log

    wait = wait_chat_log
    wait = wait and (not is_exit)
    while wait:
        time.sleep(0.01)
        wait = wait_chat_log

    time.sleep(2)
    for k in threads:
        threads[k].kill()
    subprocess.Popen(['./stopall'], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
    sys.stdout.flush()
    time.sleep(1)
    if is_exit:
        os._exit(0)
    else:
        sys.exit(0)


def timeout():
    time.sleep(120)
    exit(True)


def main():
    global threads, wait_chat_log, wait_for_ack, started_processes, debug
    timeout_thread = Thread(target=timeout, args=())
    timeout_thread.daemon = True
    timeout_thread.start()

    while True:
        line = ''
        try:
            line = sys.stdin.readline()
        except:  # keyboard exception, such as Ctrl+C/D
            exit(True)
        if line == '':  # end of a file
            exit()
        line = line.strip()  # remove trailing '\n'
        if line == 'exit':  # exit when reading 'exit' command
            if wait_for_ack:  # waitForAck wait_for_acks these commands
                time.sleep(2)
                if wait_for_ack:
                    ack_lock.acquire()
                    to_resend = wait_for_ack_list.copy()
                    ack_lock.release()
                    for m in to_resend:
                        if to_resend[m] >= 0:
                            send(to_resend[m], msgs[m])
            while wait_for_ack:
                time.sleep(0.1)
            exit()
        sp1 = line.split(None, 1)
        sp2 = line.split()
        if len(sp1) != 2:  # validate input
            continue
        pid = int(sp2[0])  # first field is pid
        cmd = sp2[1]  # second field is command
        if cmd == 'waitForAck':
            mid = int(sp2[2])
            ack_lock.acquire()
            if mid not in acked_list:
                wait_for_ack = True
                wait_for_ack_list[mid] = pid
            ack_lock.release()
        elif cmd == 'start':
            port = int(sp2[3])
            # sleep a while if a process is going to recover -- to avoid the
            # case that the process is started but the previous one hasn't
            # crashed.
            if pid not in started_processes:
                started_processes[pid] = True
            else:
                time.sleep(2)

            # start the process
            if debug:
                process = subprocess.Popen(['./process', str(pid), sp2[2], sp2[3]], preexec_fn=os.setsid)
            else:
                process = subprocess.Popen(['./process', str(pid), sp2[2], sp2[3]], stdout=open('/dev/null', 'w'),
                    stderr=open('/dev/null', 'w'), preexec_fn=os.setsid)

            # sleep for a while to allow the process be ready
            time.sleep(3)
            # connect to the port of the pid
            handler = ClientHandler(pid, address, port, process)
            threads[pid] = handler
            handler.start()
        else:
            if wait_for_ack:  # waitForAck wait_for_acks these commands
                time.sleep(2)
                if wait_for_ack:
                    ack_lock.acquire()
                    to_resend = wait_for_ack_list.copy()
                    ack_lock.release()
                    for m in to_resend:
                        if to_resend[m] >= 0:
                            send(to_resend[m], msgs[m])
            while wait_for_ack:
                time.sleep(0.1)

            if cmd == 'msg':  # message msgid msg
                msgs[int(sp2[2])] = sp1[1]
                send(pid, sp1[1])
            elif cmd[:5] == 'crash':  # crashXXX
                send(pid, sp1[1])
            elif cmd == 'get':  # get chatLog
                if not wait_chat_log:  # sleep for the first continous get command
                    time.sleep(1)
                else:
                    while wait_chat_log:  # get command blocks next get command
                        time.sleep(0.1)
                send(pid, sp1[1], set_wait=True)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'debug':
        debug = True
    main()
