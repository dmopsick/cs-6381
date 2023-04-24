# Here, I am going to create a topology like this:
sudo mn --topo=single,10
NOTE: The number after the h correlates to the ip address of the created host
EX: hn means the address will be 10.0.0.n so must be specified with the -a flag
EX: h1 -->  -a "10.0.0.1" because h1 means the ip address of host is 10.0.0.1
