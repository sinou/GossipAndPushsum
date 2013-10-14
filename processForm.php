<html>
<head>
<script language="javascript" type="text/javascript" src="jquery.min.js"></script>
<script language="javascript" type="text/javascript" src="jquery.jqplot.min.js"></script>
<link rel="stylesheet" type="text/css" href="modern.css" />

<link rel="stylesheet" type="text/css" href="jquery.jqplot.css" />
</head>
<body>
<div id="chartdiv" style="height:400px;width:300px; "></div>
<?php 
$con=mysqli_connect("localhost","root","root","logger");
// Check connection
if (mysqli_connect_errno())
  {
  echo "Failed to connect to MySQL: " . mysqli_connect_error();
  }

$result = mysqli_query($con,"SELECT * FROM log where topology = '" . $_GET['topology'] . "' AND algorithm = '" . $_GET['algorithm'] . "'" );
$size = [];
$time = [];
while($row = mysqli_fetch_array($result))
  {
    array_push($size, $row['nrofnodes']);
    array_push($time, $row['timetaken']);
  
  }
mysqli_close($con);

?>

<script language="javascript" type="text/javascript"> 
var s = new Array(<?php echo implode(',', $size); ?>);
var t = new Array(<?php echo implode(',', $time); ?>);
var array = [[]];
for(i = 0; i < s.length; i++){

  array[[i]] = [s[i], t[i]];

}
 $.jqplot('chartdiv',  [array]);
</script>
X-axis = number of nodes; Y-axis = time taken
</body>
</html>