h1 python3 DiscoveryAppln.py -P 10 -S 4 > discovery.out 2>&1 &
h2 python3 PublisherAppln.py -d "10.0.0.1:5556" -a "10.0.0.2" -T 5 -n pub1 > pub1.out 2>&1 &
h3 python3 PublisherAppln.py -d "10.0.0.1:5556" -a "10.0.0.3" -T 5 -n pub2 > pub2.out 2>&1 &
h4 python3 PublisherAppln.py -d "10.0.0.1:5556" -a "10.0.0.4" -T 5 -n pub3 > pub3.out 2>&1 &
h5 python3 PublisherAppln.py -d "10.0.0.1:5556" -a "10.0.0.5" -T 5 -n pub4 > pub4.out 2>&1 &
h6 python3 PublisherAppln.py -d "10.0.0.1:5556" -a "10.0.0.6" -T 5 -n pub5 > pub5.out 2>&1 &
h7 python3 PublisherAppln.py -d "10.0.0.1:5556" -a "10.0.0.7" -T 5 -n pub6 > pub6.out 2>&1 &
h8 python3 PublisherAppln.py -d "10.0.0.1:5556" -a "10.0.0.8" -T 5 -n pub7 > pub7.out 2>&1 &
h9 python3 PublisherAppln.py -d "10.0.0.1:5556" -a "10.0.0.9" -T 5 -n pub8 > pub8.out 2>&1 &
h10 python3 PublisherAppln.py -d "10.0.0.1:5556" -a "10.0.0.10" -T 5 -n pub9 > pub9.out 2>&1 &
h11 python3 PublisherAppln.py -d "10.0.0.1:5556" -a "10.0.0.11" -T 5 -n pub10 > pub10.out 2>&1 &
h12 python3 SubscriberAppln.py -d "10.0.0.1:5556" -T 4 -n sub1 -i 60 > sub1.out 2>&1 &
h13 python3 SubscriberAppln.py -d "10.0.0.1:5556" -T 5 -n sub2 -i 60 > sub2.out 2>&1 &
h14 python3 SubscriberAppln.py -d "10.0.0.1:5556" -T 4 -n sub3 -i 60 > sub3.out 2>&1 &
h15 python3 SubscriberAppln.py -d "10.0.0.1:5556" -T 5 -n sub4 -i 60 > sub4.out 2>&1 &
h16 python3 BrokerAppln.py -d "10.0.0.1:5556"  -a "10.0.0.16" -n broker1 > broker1.out 2>&1 &