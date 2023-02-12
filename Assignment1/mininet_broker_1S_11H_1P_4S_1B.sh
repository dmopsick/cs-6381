h1 python3 DiscoveryAppln.py -P 1 -S 4 > discovery.out 2>&1 &
h2 python3 PublisherAppln.py -d "10.0.0.1:5556" -a "10.0.0.2" -T 5 -n pub1 > pub1.out 2>&1 &
h3 python3 SubscriberAppln.py -d "10.0.0.1:5556" -T 4 -n sub1 -i 60 > sub1.out 2>&1 &
h4 python3 SubscriberAppln.py -d "10.0.0.1:5556" -T 5 -n sub2 -i 60 > sub2.out 2>&1 &
h5 python3 SubscriberAppln.py -d "10.0.0.1:5556" -T 4 -n sub3 -i 60 > sub3.out 2>&1 &
h6 python3 SubscriberAppln.py -d "10.0.0.1:5556" -T 5 -n sub4 -i 60 > sub4.out 2>&1 &
h7 ifconfig 10.0.0.7 python3 BrokerAppln.py -d "10.0.0.1:5556"  -a "10.0.0.7" -n broker1 > broker1.out 2>&1 &