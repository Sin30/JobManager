<?php

require "include.php";

//send a reverse job and compare result
$load = 'Hello World';
$result = $jobRedis->sendJob('reverse', $load, true);
$expect = strrev($load);
if($result !== $expect){
	echo "$result !== $expect", "\n";
}else{
	echo "$result === $expect", "\n";
}