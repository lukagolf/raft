# Raft

## High-Level Approach

All the project's code resides inside the Replica class, symbolizing a replica in the Raft system. I decided to execute this project in Python, both due to its simplicity and the starter code being in Python. I followed the recommended development timeline from the project website and the Raft paper to implement the protocol.

Each Raft protocol process has its dedicated method. RPCs, like AppendEntries and RequestVote, have their individual methods supplemented by several helper functions. Every kind of message in Raft gets its method. When a message arrives at a Replica, the primary message handler identifies the message type and directs it to the right handler. This method of compartmentalizing functions proved instrumental in keeping the code organized. I aimed for a "state-machine oriented" approach while implementing the replica. This means that most methods first identify the current state and act accordingly. I envisioned this project to consist of two state machines: the log system and the replica's state.

Constants are declared at the file's top, promoting organized code by eliminating repetitive manual input. Aspects like the current role of the replica are maintained as class variables, allowing every method to ascertain the system's state before proceeding.

## Challenges I Faced
This project presented several challenges. Initially, grasping the workings of Raft was tricky. Having never interacted with a protocol like Raft, comprehending its intricacies took time. Partition tests posed another challenge, primarily due to the unpredictable nature of network partitions. A considerable amount of debugging was required to ace these tests.

Another persistent challenge, not just with this project but others too, is the discrepancy between local tests and Gradescope tests. Sometimes, tests run flawlessly on my local machine but falter on Gradescope. This prompted me to minimize testing errors and focus intensely on optimal performance. Such challenges inadvertently became motivators, urging me to refine performance metrics and trim unnecessary inter-replica messages.

## Good Features
I take pride in certain features of the project. My approach of fragmenting the code functionality significantly accelerated development. For every Raft protocol component, I designed a unique helper method, ensuring distinct functionalities didn’t overlap. This organizational strategy simplified navigation and compartmentalized protocol elements, ensuring no method became overburdened.

For instance, each Raft protocol RPC gets its method along with associated helper functions. On receiving a message, a generic message handler processes it. After identifying the message type, it gets dispatched to the respective handler. Such organization streamlined the code, allowing me to focus on individual protocol elements without overwhelming interference.

## How I Tested
I utilized print statements and the provided testing script for testing. These print tests became indispensable during debugging, revealing message pathways between replicas and reflecting real-time class variable values. I consistently monitored the committed keystore and log to ensure accurate updates.

The primary testing methodology revolved around the supplied testing script and configurations. These configurations shed light on system intricacies and highlighted potential Raft protocol implementation errors. The script's statistics offered insights into the program's underperforming and well-functioning segments.

As development progressed, I leaned heavily on Gradescope tests to gauge the program's efficiency. With most local machine tests succeeding, my attention shifted to acing Gradescope tests, which ultimately determine the grade.