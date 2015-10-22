
<?php
 header("Access-Control-Allow-Origin: http://www.atmos.albany.edu");
//$var =$_POST["varname"];
   class MyDB extends SQLite3
   {
      function __construct()
      {
         $this->open('adecks');
      }
   }
   $db = new MyDB();
   if($db){
      if(isset($_POST["date"])){
	$mdate = $_POST["date"];
        settype($mdate, 'integer');  // simple check for nefarious types
	}else{
      $mdate = $db->querySingle('select max(date) from atl');
      };
//	Hopefully this way we're safe. 
        $sqlstr = $db->prepare('select distinct * from atl where fhr=0 AND tech ="CARQ" AND date <= :date AND date >= :date-12 ');
        $sqlstr->bindValue(':date', $mdate);
        $results = $sqlstr->execute();
//	$results = $db->query('select distinct * from atl where fhr=0 AND tech ="CARQ" AND date <='.$mdate.' AND date >='.$mdate.'-12');
//	  ^^ liable to injection flaws ^^
//'SELECT DISTINCT '.$var.' FROM atl WHERE date=');
      while ($row = $results->fetchArray()){
        $array[] = array(
                "fhr" => $row['date'] - $mdate,
                "tech"=>$row['tech'],
                "id"=>$row['id'],
                "mslp"=>$row['mslp'],
                "date"=>$row['date'],
                "lat"=> $row['lat'],
		"type"=>$row['type'],
                "lon"=>$row['lon']);
      } 
   echo json_encode($array);    
}
$db->close();
?>

