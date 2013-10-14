<html>
<head>
<script language="javascript" type="text/javascript" src="jquery.min.js"></script>
<script language="javascript" type="text/javascript" src="jquery.jqplot.min.js"></script>
<link rel="stylesheet" type="text/css" href="jquery.jqplot.css" />
<link rel="stylesheet" type="text/css" href="modern.css" />
</head>
<body class = "metrouicss tile" >
<div style="
    padding: 300 415;
">
<?php echo "<form action = 'processForm.php' method = 'GET'>\n
            Topology: <select name = 'topology'>
  			<option>line</option>
  			<option>2d</option>
  			<option>imp2d</option>
			<option>full</option>
		      </select>
            Algorithm: <select name = 'algorithm'>
  			<option>gossip</option>
  			<option>push-sum</option>
		      </select>
            <input type = 'submit' class = 'big' value = 'Get Graph'>"?>
</div>
</body>
</html>