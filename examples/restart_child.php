<?php

require "include.php";

//restart worker by pid
$jobRedis->sendOrder([
	'name' => 'restart_child',
	'pid' => 10011,
]);