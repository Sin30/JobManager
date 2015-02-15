<?php

require "include.php";

//start a worker
$jobRedis->sendOrder([
	'name' => 'start_child',
	'worker' => 'reverse',
]);