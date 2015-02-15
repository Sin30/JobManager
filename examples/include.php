<?php

set_time_limit(0);

require "../classes/JobRedis.php";

$jobRedis = new JobRedis('127.0.0.1', 6379);