import akka.actor._
import akka.pattern._
import akka.util.Timeout
import scala.concurrent.duration._
import java.sql.{Connection, DriverManager, ResultSet};
import java.sql.Timestamp

  sealed trait GossipPushSum
  case object Intitiate extends GossipPushSum
object Project2 {

  
  def main(args: Array[String]){
    
   
    if(args.length<3)
    {
      println("provide correct agrs")
    }
    else
    {
    val system = ActorSystem("Proj1System")
    
    val master = system.actorOf(Props(new Master(args(0).toInt, args(1), args(2), args(3).toDouble)), "Master")
    
    master ! Intitiate
    }
   }
}
   class Master(
        numNodes: Int, topology: String, algorithm: String, selfImp:Double)
    extends Actor {
     var nodeArray = new Array[ActorRef](numNodes) 
     var nrOfNodes = numNodes
     var deadNodes:List[Int] = List.empty
     var timeStart:Long = 0;  
    algorithm match
     {
       case "push-sum"=>
     for( x <- 0 until numNodes )
     {
      nodeArray(x) =  context.system.actorOf(Props(new PushSumNode(numNodes, topology, selfImp)),""+x)
     }
       case "gossip" =>
      for( x <- 0 until numNodes )
     {
      nodeArray(x) =  context.system.actorOf(Props(new GossipNode(numNodes, topology, selfImp)),""+x)
     }   
     }
     
    val system = context.system
        import system.dispatcher
      context.system.scheduler.scheduleOnce(180 seconds, self, "shutdown")
     override def preStart() = 
     {
        timeStart = System .currentTimeMillis()
     }
     def receive = {
       case Intitiate => println("Initiating... " + numNodes + " " + topology + " " + algorithm)
      
       algorithm match
     {
       case "push-sum"=>
     nodeArray(0) ! List(0.0, 0.0)	
    	
       case "gossip" =>
     nodeArray(0) ! true	
    	   
     }
    
 
       case a: Boolean => 
       deadNodes = sender.path.name.toInt:: deadNodes
         if(nrOfNodes > 1){
        nrOfNodes = nrOfNodes - 1
       }else{
         //println("Dead nodes are "+deadNodes);  
         println("All are dead for sure")
         println("Shutting system down")
         val dbc = "jdbc:mysql://localhost:3306/logger?user=root&password=root"
         classOf[com.mysql.jdbc.Driver]
         val conn = DriverManager.getConnection(dbc)
  val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
   try {
    val prep = conn.prepareStatement("INSERT INTO log (nrofnodes, topology, algorithm, timetaken, timestamp) VALUES (?, ?, ?, ?, ?) ")
    prep.setInt(1, numNodes)
    prep.setString(2, topology)
    prep.setString(3, algorithm)
    prep.setInt(4, (System.currentTimeMillis() - timeStart).toInt)
    var date: java.util.Date = new java.util.Date();
    prep.setTimestamp(5, new Timestamp(date.getTime()))
    prep.executeUpdate
  }
  finally {
    conn.close
  }
         
           
         context.system.shutdown();
       }
       case "shutdown" => println("timeout")
       println("Dead nodes are "+deadNodes);  
       println("Number of alive are " + nrOfNodes);
       context.system.shutdown()
     }
     override def postStop()
      {
       println("Time taken is "+ (System.currentTimeMillis() - timeStart)+" msecs")  
      }
     
   }
 
  
 
 class PushSumNode(numNodes: Int, topology: String,var  selfImp: Double) extends Actor
 {
   
  var nodeNum:Int = 0 
  var validNeighbours:Int = 0;
  var invalidNeighbours:List[Int] = List.empty
  var root:Int =0;
  var gossipStart:Boolean = false;
  var dead:Boolean = false
  var schedulerFlag:Cancellable = null;
  var randomNode:Int=0;
  val system = context.system
  var s: Double = 0
  var w: Double = 1
  var prevsw:Double=0    
  var consec:Int =0
  var sw :Double = 0
  override def preStart()
  {

    nodeNum = self.path.name.toInt;
    s = nodeNum +1
    sw= s/w
	  topology match
    {
      case "2d" =>
      validNeighbours = 4;  
      root =  Math.ceil(Math.sqrt(numNodes.toDouble)).toInt
      case "line" =>
	  validNeighbours = 2;
      case "imp2d"=>
      validNeighbours = 5;
       root =  Math.ceil(Math.sqrt(numNodes.toDouble)).toInt
      generateRandomConnection();
      case full =>
      validNeighbours = numNodes;
        
    }

  }
  def receive = {
    case List(a: Double, b: Double)  =>
      
      prevsw =sw
      s = s + a
       w = w + b  
       sw = s/w
    
    if(gossipStart == false)
      {
       import system.dispatcher  
      schedulerFlag=  context.system.scheduler.schedule(0 seconds, 5 milliseconds, self, "ping"+topology)
      
      }
      gossipStart = true
      if((prevsw- (sw)).abs>.0000000001 ){
       consec = 0;
      }
      
      else
      {
        if(consec == 3)
        {
        if(!dead)
        {
       
       println(nodeNum +" Dead by s/w : "+ sw +" s : "+ s +" w: "+ w)
        schedulerFlag.cancel;
       
        context.actorSelection("../Master") ! false
      //context.actorSelection("../" + nodeNum) ! Kill
        }
        else
        {
          sender ! false
        }
        dead = true
        }
        consec = consec+1;
      }
      
  /*  case List(a: Double, b: Double) => 
      					s = s + a
      					w = w + b*/

    case false =>
      if(topology!="full")
      {
    	  if(!invalidNeighbours.contains(sender.path.name))
    		  invalidNeighbours =sender.path.name.toInt::invalidNeighbours
      }
     case "pingfull" =>
    sendInfofull();
    
    case "ping2d" =>
    sendInfo2D();
    case "pingimp2d" =>
    sendInfoimp2D();
    case "pingline" =>
    sendInfoLine();

  }
  
  def sendInfo2D()
  {
    var temp =  select2DNeighbour()
    	s = s/2
    	w = w/2
        sendMessage(temp, List(s, w));
    

 
     //println("Sender GossipNode num: " + nodeNum + " Gossip Count : "+gossipCount + " Rec : "+ temp)
     
  }
  def select2DNeighbour():Int =
  { 
    var selec:Int =0;
    var dir =0;
    do
    {
      dir = pickANeighbour(validNeighbours)
      selec =getNodeNumber2D(dir);      
      
      if(!checkIfValid2DNeighbour(dir)&& !invalidNeighbours.contains(selec) )
      {
        
        invalidNeighbours = selec:: invalidNeighbours
      }
     
    }while(!(dir == validNeighbours +1 || !invalidNeighbours.contains(selec)))
      selec
    
  }
def getNodeNumber2D(dir:Int):Int=
{
  var selec = 0;
  dir match{
          case 1 => selec = nodeNum -1;//println("Here3")
          case 2 => selec = nodeNum -root;//println("Here4")
          case 3 => selec = nodeNum +1;//println("Here5")
          case 4 => selec = nodeNum +root;//println("Here6")
          case 5 => selec = nodeNum
        }
selec
  }

   def checkIfValid2DNeighbour( dir: Int): Boolean = {
    var temp: Int = 0
    var myRowNum: Int = 0
    var newRowNum: Int = 0
    dir match{
      case 1 | 3 => if(dir == 1)
    	  				temp = nodeNum - 1   //W
    	  			else
    	  			    temp = nodeNum + 1   // E
                myRowNum = Math.ceil(nodeNum/root).toInt
                newRowNum = Math.ceil(temp/root).toInt
		        if(temp >= 0 && temp < numNodes && myRowNum == newRowNum ){
		          true
		        }else{
		          false
		        }
      case 2 | 4 => if(dir == 2)
    	  				temp = nodeNum - root  //N
    	  			else
    	  			    temp = nodeNum + root  //S
    		  	if(temp >= 0 && temp < numNodes){
    		  	  true
    		  	}else{
    		  	  false
    		  	}
      case 5 =>
        true
    }
    
   
  }
def sendInfoLine(){
    var temp = selectLineNeighbour()
    	s = s/2
    	w = w/2
        sendMessage(temp, List(s, w));
    
    
  }
def checkIfValidLineNeighbour(dir: Int, selected: Int): Boolean = {
     if(selected >= 0){
     dir match{
       case 1 => 
                 if(nodeNum + 1 <= numNodes)
                          true
                 else
                	 	  false
       case 2 =>
                 if(nodeNum - 1 >= 0)
                          true
                 else
                          false
                          
       case 3 => 
                 true
                	 	  
     }
     }else{
       false
     }
   }
 def selectLineNeighbour(): Int = {
    
    var selected: Int = 0
    var dir: Int = 0
    do{
      dir = pickANeighbour(validNeighbours)
      selected =getNodeNumberLine(dir); 
    if(checkIfValidLineNeighbour(dir, selected)&& !invalidNeighbours.contains(selected))
      {
        
        invalidNeighbours = selected:: invalidNeighbours
      }
    }while( !invalidNeighbours.contains(selected) )
    
    
    selected
  }
 
 def getNodeNumberLine(dir:Int):Int=
{
  var selected = 0;
  
  dir match{
          case 1 => selected = nodeNum + 1;
          case 2 => selected = nodeNum - 1
          case 3 => selected = nodeNum 
          
        }
  
selected
  }
 def generateRandomConnection()
 {
   randomNode = math.floor(math.random * numNodes).toInt;
 while(randomNode==nodeNum)
 {
   randomNode = math.floor(math.random * numNodes).toInt;
 }
 }
  def sendInfoimp2D()
  {
    var temp =  selectimp2DNeighbour()
        sendMessage(temp, true);
    	s = s/2
    	w = w/2
        sendMessage(temp, List(s, w));
    
  }
  def selectimp2DNeighbour():Int =
  { 
    var selec:Int =0;
    var dir =0;
    do
    {
      dir = pickANeighbour(validNeighbours)
      selec =getNodeNumberimp2D(dir);      
      
      if(!checkIfValidimp2DNeighbour(dir)&& !invalidNeighbours.contains(selec) )
      {
        
        invalidNeighbours = selec:: invalidNeighbours
      }
     
    }while(invalidNeighbours.contains(selec))
      selec
    
  }
def getNodeNumberimp2D(dir:Int):Int=
{
  var selec = 0;
  dir match{
          case 1 => selec = nodeNum -1;//println("Here3")
          case 2 => selec = nodeNum -root;//println("Here4")
          case 3 => selec = nodeNum +1;//println("Here5")
          case 4 => selec = nodeNum +root;//println("Here6")
          case 5 => selec = nodeNum
          case 6 => selec = randomNode
  }
selec
  }

   def checkIfValidimp2DNeighbour( dir: Int): Boolean = {
    var temp: Int = 0
    var myRowNum: Int = 0
    var newRowNum: Int = 0
    dir match{
      case 1 | 3 => if(dir == 1)
    	  				temp = nodeNum - 1   //W
    	  			else
    	  			    temp = nodeNum + 1   // E
                myRowNum = Math.ceil(nodeNum/root).toInt
                newRowNum = Math.ceil(temp/root).toInt
		        if(temp >= 0 && temp < numNodes && myRowNum == newRowNum ){
		          true
		        }else{
		          false
		        }
      case 2 | 4 => if(dir == 2)
    	  				temp = nodeNum - root  //N
    	  			else
    	  			    temp = nodeNum + root  //S
    		  	if(temp >= 0 && temp < numNodes){
    		  	  true
    		  	}else{
    		  	  false
    		  	}
      case 5 |6=>
        true
    }
    
   
  }
  def sendInfofull()
  {
    var temp =  selectfullNeighbour()
        sendMessage(temp, true);
 
	s = s/2
   	w = w/2
    sendMessage(temp, List(s, w));
         
  }
  def selectfullNeighbour():Int =
  { 
    var selec:Int =0;
    var dir =0;
    selec = pickANeighbour(validNeighbours) -1
      selec
    
  }
 

  def pickANeighbour(numNeighbour:Int):Int =  math.ceil((math.random)/((1.0)/(numNeighbour.toDouble + selfImp))).toInt
  def sendMessage(nodeNum:Int, Message:Any)
  {
   findActor(nodeNum.toString) ! Message
  }
  def findActor(nodeName:String):ActorSelection =   context.actorSelection("../"+nodeName)
 }
 class GossipNode(numNodes: Int, topology: String,var  selfImp: Double) extends Actor
 {
   
  var nodeNum:Int = 0 
  var validNeighbours:Int = 0;
  var invalidNeighbours:List[Int] = List.empty
  var root:Int =0;
  var gossipCount: Int = 0;
  var dead:Boolean = false
  var schedulerFlag:Cancellable = null;
  var randomNode:Int=0;
  val system = context.system
        
  override def preStart()
  {

    nodeNum = self.path.name.toInt;
	  topology match
    {
      case "2d" =>
      validNeighbours = 4;  
      root =  Math.ceil(Math.sqrt(numNodes.toDouble)).toInt
      case "line" =>
	  validNeighbours = 2;
      case "imp2d"=>
      validNeighbours = 5;
       root =  Math.ceil(Math.sqrt(numNodes.toDouble)).toInt
      generateRandomConnection();
      case full =>
      validNeighbours = numNodes;
        
    }

  }
  def receive = {
    case true  =>
     
      if(gossipCount == 0)
      {
       import system.dispatcher 
      schedulerFlag=  context.system.scheduler.schedule(0 seconds, 5 milliseconds, self, "ping"+topology)
      }
      if(gossipCount < (10-1) ){
       gossipCount += 1;
      }
      
      else
      {
        if(!dead)
        {
          
        schedulerFlag.cancel;
       
        context.actorSelection("../Master") ! false
      //context.actorSelection("../" + nodeNum) ! Kill
        }
        else
        {
          sender ! false
        }
        dead = true
      }

    case false =>
      if(topology!="full")
      {
    	  if(!invalidNeighbours.contains(sender.path.name))
    		  invalidNeighbours =sender.path.name.toInt::invalidNeighbours
      }
     case "pingfull" =>
    sendInfofull();
    
    case "ping2d" =>
    sendInfo2D();
    case "pingimp2d" =>
    sendInfoimp2D();
    case "pingline" =>
    sendInfoLine();

  }
  
  def sendInfo2D()
  {
    var temp =  select2DNeighbour()
        sendMessage(temp, true);

 
     //println("Sender Node num: " + nodeNum + " Gossip Count : "+gossipCount + " Rec : "+ temp)
     
  }
  def select2DNeighbour():Int =
  { 
    var selec:Int =0;
    var dir =0;
    do
    {
      dir = pickANeighbour(validNeighbours)
      selec =getNodeNumber2D(dir);      
      
      if(!checkIfValid2DNeighbour(dir)&& !invalidNeighbours.contains(selec) )
      {
        
        invalidNeighbours = selec:: invalidNeighbours
      }
     
    }while(!(dir == validNeighbours +1 || !invalidNeighbours.contains(selec)))
      selec
    
  }
def getNodeNumber2D(dir:Int):Int=
{
  var selec = 0;
  dir match{
          case 1 => selec = nodeNum -1;//println("Here3")
          case 2 => selec = nodeNum -root;//println("Here4")
          case 3 => selec = nodeNum +1;//println("Here5")
          case 4 => selec = nodeNum +root;//println("Here6")
          case 5 => selec = nodeNum
        }
selec
  }

   def checkIfValid2DNeighbour( dir: Int): Boolean = {
    var temp: Int = 0
    var myRowNum: Int = 0
    var newRowNum: Int = 0
    dir match{
      case 1 | 3 => if(dir == 1)
    	  				temp = nodeNum - 1   //W
    	  			else
    	  			    temp = nodeNum + 1   // E
                myRowNum = Math.ceil(nodeNum/root).toInt
                newRowNum = Math.ceil(temp/root).toInt
		        if(temp >= 0 && temp < numNodes && myRowNum == newRowNum ){
		          true
		        }else{
		          false
		        }
      case 2 | 4 => if(dir == 2)
    	  				temp = nodeNum - root  //N
    	  			else
    	  			    temp = nodeNum + root  //S
    		  	if(temp >= 0 && temp < numNodes){
    		  	  true
    		  	}else{
    		  	  false
    		  	}
      case 5 =>
        true
    }
    
   
  }
def sendInfoLine(){
    var temp = selectLineNeighbour()
    		sendMessage(temp, true);
    //println("Sender Node num: " + nodeNum + " Gossip Count : "+gossipCount + " Rec : "+ temp)
  }
def checkIfValidLineNeighbour(dir: Int, selected: Int): Boolean = {
     if(selected >= 0){
     dir match{
       case 1 => 
                 if(nodeNum + 1 <= numNodes)
                          true
                 else
                	 	  false
       case 2 =>
                 if(nodeNum - 1 >= 0)
                          true
                 else
                          false
                          
       case 3 => 
                 true
                	 	  
     }
     }else{
       false
     }
   }
 def selectLineNeighbour(): Int = {
    
    var selected: Int = 0
    var dir: Int = 0
    do{
      dir = pickANeighbour(validNeighbours)
      selected =getNodeNumberLine(dir); 
    if(checkIfValidLineNeighbour(dir, selected)&& !invalidNeighbours.contains(selected))
      {
        
        invalidNeighbours = selected:: invalidNeighbours
      }
    }while( !invalidNeighbours.contains(selected) )
    
    
    selected
  }
 
 def getNodeNumberLine(dir:Int):Int=
{
  var selected = 0;
  
  dir match{
          case 1 => selected = nodeNum + 1;
          case 2 => selected = nodeNum - 1
          case 3 => selected = nodeNum 
          
        }
  
selected
  }
 def generateRandomConnection()
 {
   randomNode = math.floor(math.random * numNodes).toInt;
 while(randomNode==nodeNum)
 {
   randomNode = math.floor(math.random * numNodes).toInt;
 }
 //println("Random Node for "+ nodeNum +" is " + randomNode )
 }
  def sendInfoimp2D()
  {
    var temp =  selectimp2DNeighbour()
        sendMessage(temp, true);
 
     //println("Sender Node num: " + nodeNum + " Gossip Count : "+gossipCount + " Rec : "+ temp)
     
  }
  def selectimp2DNeighbour():Int =
  { 
    var selec:Int =0;
    var dir =0;
    do
    {
      dir = pickANeighbour(validNeighbours)
      selec =getNodeNumberimp2D(dir);      
      
      if(!checkIfValidimp2DNeighbour(dir)&& !invalidNeighbours.contains(selec) )
      {
        
        invalidNeighbours = selec:: invalidNeighbours
      }
     
    }while(invalidNeighbours.contains(selec))
      selec
    
  }
def getNodeNumberimp2D(dir:Int):Int=
{
  var selec = 0;
  dir match{
          case 1 => selec = nodeNum -1;//println("Here3")
          case 2 => selec = nodeNum -root;//println("Here4")
          case 3 => selec = nodeNum +1;//println("Here5")
          case 4 => selec = nodeNum +root;//println("Here6")
          case 5 => selec = nodeNum
          case 6 => selec = randomNode
  }
selec
  }

   def checkIfValidimp2DNeighbour( dir: Int): Boolean = {
    var temp: Int = 0
    var myRowNum: Int = 0
    var newRowNum: Int = 0
    dir match{
      case 1 | 3 => if(dir == 1)
    	  				temp = nodeNum - 1   //W
    	  			else
    	  			    temp = nodeNum + 1   // E
                myRowNum = Math.ceil(nodeNum/root).toInt
                newRowNum = Math.ceil(temp/root).toInt
		        if(temp >= 0 && temp < numNodes && myRowNum == newRowNum ){
		          true
		        }else{
		          false
		        }
      case 2 | 4 => if(dir == 2)
    	  				temp = nodeNum - root  //N
    	  			else
    	  			    temp = nodeNum + root  //S
    		  	if(temp >= 0 && temp < numNodes){
    		  	  true
    		  	}else{
    		  	  false
    		  	}
      case 5 |6=>
        true
    }
    
   
  }
  def sendInfofull()
  {
    var temp =  selectfullNeighbour()
        sendMessage(temp, true);
 
     //println("Sender Node num: " + nodeNum + " Gossip Count : "+gossipCount + " Rec : "+ temp)
     
  }
  def selectfullNeighbour():Int =
  { 
    var selec:Int =0;
    var dir =0;
    selec = pickANeighbour(validNeighbours) -1
      selec
    
  }
 

  def pickANeighbour(numNeighbour:Int):Int =  math.ceil((math.random)/((1.0)/(numNeighbour.toDouble + selfImp))).toInt
  def sendMessage(nodeNum:Int, Message:Any)
  {
   findActor(nodeNum.toString) ! Message
  }
  def findActor(nodeName:String):ActorSelection =   context.actorSelection("../"+nodeName)
 }





