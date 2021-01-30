# Pastry
## Successfully implemented Pastry APIs for network join and routing as mentioned in the paper.<br />
• The neighborhood set for the first set of M nodes get initialized with all the other nodes present in the said set. Although in the pastry paper, the neighborhood set initialization for a new node is done geographically, we implement it by selecting a random node from the existing network and initialize with its neighborhood set.<br />
• Each node sends a request per second numRequests number of times.<br />
• The constants in the program Pastry.fsx, b(base of NodeId), l(length of NodeId) and M(Size of the leaf and neighborhood set) can be modified (b should be within 2 to 10) to check the results with multiple values.<br />
• For the final output: By default, we print the average number of hops traversed to deliver a message. For a detailed statistic the third parameter can be entered as 1. This groups the messages delivered with respect to number of hops taken and displays the info.<br />
