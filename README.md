# SOF37001
SOF37001 Uni


II.a - Application 1: Simple Message Broker (MB) Application using Python

Hypothetical Budget: £10,000.00 - £50,000.00
Project timescale: 3 months

“A message broker (also known as an integration broker or interface engine) is an intermediary computer
program module that translates a message from the formal messaging protocol of the sender to the formal
messaging protocol of the receiver. Message brokers are elements in telecommunication or computer
networks where software applications communicate by exchanging formally-defined messages. Message
brokers are a building block of message-oriented middleware (MOM) but are typically not a replacement
for traditional middleware like MOM and remote procedure call (RPC).” (Ref: Wiki).
According to IBM, “A message broker is software that enables applications, systems, and services to
communicate with each other and exchange information. The message broker does this by translating
messages between formal messaging protocols. This allows interdependent services to “talk” with one
another directly, even if they were written in different languages or implemented on different platforms.
Message brokers are software modules within messaging middleware or message-oriented middleware
(MOM) solutions. This type of middleware provides developers with a standardized means of handling
the flow of data between an application’s components so that they can focus on its core logic. It can serve
as a distributed communications layer that allows applications spanning multiple platforms to
communicate internally. Message brokers can validate, store, route, and deliver messages to the
appropriate destinations. They serve as intermediaries between other applications, allowing senders to
issue messages without knowing where the receivers are, whether or not they are active, or how many of
them there are. This facilitates decoupling of processes and services within systems. In order to provide
reliable message storage and guaranteed delivery, message brokers often rely on a substructure or
component called a message queue  that stores and orders the messages until the consuming applications
can process them. In a message queue, messages are stored in the exact order in which they were
transmitted and remain in the queue until receipt is confirmed. Asynchronous (15:11) refers to the type
of inter-application communication that message brokers make possible. It prevents the loss of valuable
data and enables systems to continue functioning even in the face of the intermittent connectivity or
latency issues common on public networks . Asynchronous messaging guarantees that messages will be
delivered once (and once only) in the correct order relative to other messages. Message brokers may
comprise queue managers to handle the interactions between multiple message queues, as well as services
providing data routing, message translation, persistence, and client state management functionalities.”

Message Broker Software example:
 Amazon MQ
 Apache Kafka
 RabbitMQ

Your task is to analysis, design, implement and test (using unit testing) a simple MB application which
does below basic functionalities ( you should use using command line arguments in the console app).
more functionalities are found in the analysis phase).
 
 Install the application (and all dependencies)
 
 Start the environment in a separate console
 
 Create a topic to store events with number of partitions
 
 View topics, details of the topic and messages inside each topic
 
 Producer: Write some events into the topic in Json format (in a separate console)
 
 Consumer: Read the events (in a separate console)
 
 Write a test application to use the MB app to produce 100 message per second and another test
application to 4 consumers (with 2 consumer group) to read the messages (each has its own offset)
 
 (NF) The application should be user friendly, with high performance and secure. Using a good
exception handling(validation) and display meaningful message to user is required. It is required to
develop and GUI app