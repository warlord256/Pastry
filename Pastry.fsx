#time
#r "nuget: Akka.FSharp"

open System
open System.Collections.Generic
open Akka.FSharp
open Akka.Actor

let b = 10 // Base in range 2 to 10
let l = 6 // Length of node id
let M = 8 // This is the size of leaf set and the neighborhood set

type MasterMessages = 
    | Initialize of int*int*bool
    | Delivered of int

type State = {
    NodeId: string
    LowerLeaf: HashSet<string>
    HigherLeaf: HashSet<string>
    RoutingTable: string [,]
    Neighbourhood: HashSet<string>
}

type NodeMessages = 
    | PastryInit of IActorRef*string
    | Join of string*IActorRef
    | CurrentState of State*bool 
    | Arrived of State 
    | NewLeaves of HashSet<string>
    | PrintState
    | Route of int*string
    | Forward of int*int*int
    | InitializeNeighbourhood of HashSet<string>


let sharedIdLength (a:string, b:string) = 
    let mutable i=0
    let mutable count=0
    let padA=a.PadLeft(l,'0')
    let padB=b.PadLeft(l,'0')
    while i<l && padA.[i]=padB.[i] do
        count <- count+1
        i <- i+1
    count
    
let getActorById (context: IActorContext, id:string) = 
    context.ActorSelection("akka://ActorFactory/user/master/"+ id)

let getClosestNode (destNodeId:string, state:State) =
    if destNodeId=state.NodeId then
        ""
    else
        let mutable maxVal = sharedIdLength (destNodeId, state.NodeId)
        let mutable nextNodeId = ""
        for id in state.LowerLeaf do
            let diff = sharedIdLength (id, destNodeId)
            if  diff > maxVal then
                maxVal <- diff
                nextNodeId <- id
        for id in state.HigherLeaf do
            let diff = sharedIdLength (id, destNodeId)
            if  diff > maxVal then
                maxVal <- diff
                nextNodeId <- id
        if nextNodeId <>"" then
            nextNodeId
        else 
            let index = (int destNodeId.[maxVal])-48  
            if maxVal < l  && state.RoutingTable.[maxVal, index] <> ""  then
                state.RoutingTable.[maxVal, index]
            else
                nextNodeId
            


let getEquivalentNode (destNodeId:string, state:State) = 
    // printfn "I am calculating equivalent node"
    let set = if state.NodeId < destNodeId then state.HigherLeaf else state.LowerLeaf
    let mutable maxVal = sharedIdLength (destNodeId, state.NodeId)
    let mutable nextNodeId = ""
    for id in set do
        let diff = sharedIdLength (id, destNodeId)
        if  diff >= maxVal  && (abs (int destNodeId - int state.NodeId))>(abs (int destNodeId - int id)) then
            maxVal <- diff
            nextNodeId <- id
    nextNodeId

let addToRoutingTableIfFree (state: State, id:string) =
    if state.NodeId <> id then
        let matchLen = sharedIdLength (state.NodeId, id)
        let index = (int id.[matchLen]) - 48
        let entry = state.RoutingTable.[matchLen, index]
        if entry = "" || state.LowerLeaf.Contains(entry) || state.HigherLeaf.Contains(entry) then
            state.RoutingTable.SetValue(id, matchLen, index)

let replaceLeafIfCloser (state:State, nodeId:string) =
    let set = if state.NodeId < nodeId then state.HigherLeaf else state.LowerLeaf
    let mutable min = sharedIdLength (nodeId, state.NodeId)
    let mutable toReplace = ""
    for id in set do
        let com = sharedIdLength (id, state.NodeId)
        if com < min then
            toReplace <- id
            min <- com
    if toReplace <> "" then 
        set.Remove(toReplace) |> ignore
        set.Add(nodeId) |> ignore
        addToRoutingTableIfFree(state, toReplace)
        true
    else 
        false

let checkForInclusionInLeafSet (state:State, id:string) =
    let set = if state.NodeId < id then state.HigherLeaf else state.LowerLeaf
    if state.NodeId=id || set.Contains(id) then
        false
    else
        if set.Count < 8 then 
            set.Add(id) |> ignore
            true
        else
            replaceLeafIfCloser(state, id)

let getAllNodesInState(state) = 
    let nodeList = HashSet<string>()
    nodeList.UnionWith(state.LowerLeaf)
    nodeList.UnionWith(state.HigherLeaf)
    nodeList.UnionWith(state.Neighbourhood)
    for i in [0..l-1] do
        for j in [0..b-1] do
            let id = state.RoutingTable.[i,j]
            if id <> "" then
                nodeList.Add(id) |> ignore
    nodeList

let broadcastLeaves (state:State, mailbox:Actor<_>) =
    let nodes = getAllNodesInState(state)
    let leafSet = new HashSet<string>(state.HigherLeaf)
    leafSet.UnionWith(state.LowerLeaf)
    for node in nodes do
        getActorById(mailbox.Context, node) <! NewLeaves(leafSet) 

let removeRandomNodeFromSet(collection:HashSet<_>)  = 
    let rand = Random();
    let index = rand.Next(collection.Count)
    let mutable count = 0
    let mutable toRemove = null
    for i in collection do
        if count = index then
            toRemove <- i
        count <- count+1 
    if not (isNull toRemove) then
        collection.Remove(toRemove) |> ignore

let getRandomNodeFromSet(collection:HashSet<string>)  = 
    let rand = Random();
    let index = rand.Next(collection.Count)
    let mutable count = 0
    let mutable toReturn = null
    for i in collection do
        if count = index then
            toReturn <- i
        count <- count+1 
    toReturn
    


let nodeActor state (mailbox:Actor<_>) = 
    let rec loop() = actor {
        let! message = mailbox.Receive()
        try 
            match message with
            | Join (joiningNodeId, joiningNode) ->
                let nodeIdToSend = getClosestNode (joiningNodeId,state)
                let clonedState = {
                    NodeId= state.NodeId
                    LowerLeaf=state.LowerLeaf
                    HigherLeaf = state.HigherLeaf
                    RoutingTable = state.RoutingTable
                    Neighbourhood = new HashSet<string>(state.Neighbourhood)
                }
                if nodeIdToSend <> "" then
                    let node = getActorById (mailbox.Context, nodeIdToSend)
                    node <! message
                    joiningNode <! CurrentState(clonedState, false)
                else
                    joiningNode <! CurrentState(clonedState, true)
            | PastryInit(neighbour, neighbourId) -> 
                neighbour <! Join(state.NodeId, mailbox.Self)
                state.Neighbourhood.Add(neighbourId) |> ignore
            | CurrentState (remoteNodeState, isFinalNode) ->
                let remoteNodeId = remoteNodeState.NodeId
                if state.Neighbourhood.Contains(remoteNodeId) then
                    state.Neighbourhood.UnionWith(remoteNodeState.Neighbourhood)
                    while state.Neighbourhood.Count > M do
                        removeRandomNodeFromSet(state.Neighbourhood)
                let matchLen = sharedIdLength (state.NodeId, remoteNodeId)
                for i in [0..matchLen-1] do
                    for j in [0..1] do
                        if state.RoutingTable.[i, j] = "" && remoteNodeState.RoutingTable.[i,j] <> state.NodeId then
                            state.RoutingTable.SetValue(remoteNodeState.RoutingTable.[i,j], i, j) 
                if isFinalNode then
                    for id in remoteNodeState.LowerLeaf do
                        if id <> state.NodeId then
                            checkForInclusionInLeafSet(state, id) |> ignore
                            addToRoutingTableIfFree(state, id)

                    for id in remoteNodeState.HigherLeaf do
                        if id <> state.NodeId then
                            checkForInclusionInLeafSet(state, id) |> ignore
                            addToRoutingTableIfFree(state, id)

                    checkForInclusionInLeafSet(state, remoteNodeId) |> ignore
                    addToRoutingTableIfFree(state, remoteNodeId)
                       
                    // Send Arrived to all nodes in state.
                    let nodeList = getAllNodesInState(state)

                    for id in nodeList do
                        getActorById(mailbox.Context, id) <!  Arrived (state)

            | Arrived (newNodeState) ->
                addToRoutingTableIfFree(state, newNodeState.NodeId)
                if checkForInclusionInLeafSet(state, newNodeState.NodeId) then
                    broadcastLeaves(state, mailbox)
                if newNodeState.Neighbourhood.Contains(state.NodeId) then
                    while state.Neighbourhood.Count >= M/2 do
                        removeRandomNodeFromSet(state.Neighbourhood)
                    state.Neighbourhood.Add(newNodeState.NodeId) |> ignore
            | PrintState ->
                printfn "%A" state
            | Route(numOfHops, destId) ->
                if destId=state.NodeId then 
                    mailbox.Context.Parent <! Delivered(numOfHops) 
                else 
                    let mutable nextId = getClosestNode (destId, state)
                    if nextId = "" then 
                        nextId <- getEquivalentNode(destId, state)
                    if nextId <> "" then 
                        getActorById(mailbox.Context, nextId) <! Route(numOfHops+1, destId)
                    else
                        mailbox.Context.Parent <! Delivered(-1) // Change Here!
                        // getActorById(mailbox.Context, getRandomNodeFromSet(state.Neighbourhood)) <! Route(numOfHops+1, destId)
            | NewLeaves(leafSet) -> 
                let mutable leavesChanged = false
                for node in leafSet do
                    leavesChanged <- (leavesChanged || checkForInclusionInLeafSet(state, node))
                    addToRoutingTableIfFree(state, node) |> ignore
                if leavesChanged then
                    broadcastLeaves(state, mailbox)
            | InitializeNeighbourhood (neighbours) ->
                // printfn "init %s" state.NodeId
                state.Neighbourhood.UnionWith(neighbours)
                state.Neighbourhood.Remove(state.NodeId) |> ignore
            | _ -> printfn "Okay!"
        with
            | :? InvalidOperationException -> printfn "."
        return! loop()
    }
    loop()

let convertBase num=
    if b<>2 && b<>10 then
        let mutable id=num
        let mutable result =""
        while id>0 do   
            result<- (string)(id%b) + result
            id<- id/b
        result.PadLeft(l,'0')
    else
        (Convert.ToString(num,b)).PadLeft(l,'0')


let initializeNeighbourhood (nodes: List<IActorRef>, nodeIdList: List<string>) = 
    for i = 0 to nodes.Count-1 do
        nodes.[i] <! InitializeNeighbourhood(new HashSet<string>(nodeIdList))

    

let createNetwork (numberOfNodes:int, context: IActorContext) =
    let rand = System.Random()
    let maxVal = int (Math.Pow ((float b), (float l)))
    printfn "Creating network..."
    let nodeIds = new HashSet<int>()
    let nodeIdStrList = new List<string>()
    let nodeList = new List<IActorRef>()

    for i = 1 to numberOfNodes do
        let mutable id = rand.Next(0,maxVal)
        while nodeIds.Contains(id) do
            id <- rand.Next(0,maxVal)
        // printfn "Creating %d" id
        let mutable idStr = convertBase id
        let newNode = spawn context idStr <| nodeActor {
            NodeId= idStr
            LowerLeaf=new HashSet<string>();
            HigherLeaf = new HashSet<string>();
            RoutingTable = Array2D.init l b <| fun i j -> ""
            Neighbourhood = new HashSet<string>();
        }

        if i > 1 then
            let index = rand.Next(nodeList.Count)
            newNode <! PastryInit(nodeList.Item(index), nodeIdStrList.[index])
            if i=M+1 then
                initializeNeighbourhood(nodeList, nodeIdStrList)
            if i<25 then
                Async.RunSynchronously <| Async.Sleep 1000
            
        nodeIds.Add (id) |> ignore
        nodeIdStrList.Add (idStr) |> ignore
        nodeList.Add(newNode)
    (nodeList, new List<string>(nodeIdStrList))

let sendMessages (numberOfMessages:int, nodeList:List<IActorRef>, nodeIdList:List<string>) =
    Async.RunSynchronously <| Async.Sleep 5000
    printfn "Starting to send messages"
    let rand = System.Random()
    let count = nodeList.Count
    for i = 1 to numberOfMessages do
        for j = 0 to count-1 do
            let mutable index = rand.Next(count)
            while index = j do
                index <- rand.Next(count)
            nodeList.[j] <! Route(0, nodeIdList.[index])
        Async.RunSynchronously <| Async.Sleep 1000

let printStatistics (statisticsMap: Dictionary<int, int>, printDetailedStat: bool) =
    printfn "Statistics:"
    let mutable hopSum=0
    let mutable totMess=0
    for itr in statisticsMap do
        if printDetailedStat then
            printfn"| Hops taken: %d || No. of Messages: %d |" itr.Key itr.Value
        if itr.Key>0 then
            hopSum <- hopSum + (itr.Key * itr.Value)
            totMess <- totMess + itr.Value
    let avg = (float hopSum / float totMess)
    if printDetailedStat then
        printfn "The average hops taken for a total of %d messages is: %.3f"totMess avg
    else
        printfn "The average number of hops taken is %.3f" avg

let printStates(nodeList:List<IActorRef>)=
    printfn "The states are as follows: "
    for itr in nodeList do
        itr <! PrintState 
        Async.RunSynchronously <| Async.Sleep(200)
    
let supervisorActor (mailbox: Actor<_>) =
    let statisticsMap = new Dictionary<int, int>()
    let rec loop(remainingMessageCount, printDetailedStat, parent) = actor {
        let! message = mailbox.Receive()
        match message with 
        | Initialize(numberOfNodes, numberOfMessages, printDetailedStat) ->
            let (nodeList, nodeIdList) = createNetwork (numberOfNodes, mailbox.Context)
            sendMessages (numberOfMessages, nodeList, nodeIdList)
            // printStates nodeList
            return! loop(numberOfMessages*numberOfNodes, printDetailedStat, mailbox.Sender())
        | Delivered (numberOfHops) ->
            let count = statisticsMap.GetValueOrDefault(numberOfHops, 0)
            statisticsMap.Item(numberOfHops) <- count+1
            if remainingMessageCount=1 then
                printStatistics (statisticsMap, printDetailedStat)
                parent <! 1
            return! loop (remainingMessageCount-1, printDetailedStat, parent)
        return! loop (remainingMessageCount, false, parent)
    }
    loop (0, false, null)

let system = ActorSystem.Create("ActorFactory")

let args = fsi.CommandLineArgs
let numberOfNodes = int args.[1]
let numberOfMessages = int args.[2]

let prtintDetailedStat = (args.Length > 3 && args.[3]="1")

if (Math.Pow(float b, float l)) <  float numberOfNodes then
    printfn "Invalid number of nodes for b = %d and l = %d" b l
else
    let master = spawn system "master" supervisorActor
    let t = (master <? Initialize(numberOfNodes, numberOfMessages, prtintDetailedStat))
    Async.RunSynchronously t |> ignore

system.Terminate()

