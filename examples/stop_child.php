<?php

require "include.php";

//stop a worker by pid
$jobRedis->sendOrder([
	'name' => 'stop_child',
	'pid' => 10029,
]);