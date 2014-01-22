Darkseid
========

Actor-based library to help you push data into an **Amazon Kinesis** stream by taking care of the heavy lifting for you.

This guide contains the following sections:


You can check out the release note [here](https://github.com/theburningmonk/Darkseid/blob/develop/RELEASE_NOTES.md), and report issues [here](https://github.com/theburningmonk/darkseid/issues).





## The Basics

#### Before you start

Please familiarize yourself with how **Amazon Kinesis** works by looking through its online [documentations](http://aws.amazon.com/documentation/kinesis/), in particular its [Key Concepts](http://docs.aws.amazon.com/kinesis/latest/dev/key-concepts.html) and [Limitations](http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html).


#### Getting Started

Download and install the library from Nuget.



This library enables you to write a data producing application on top of Amazon Kinesis.

To start, you need an instance of `IProducer`, which you can create using one of the overloads of the static method `Producer.CreateNew`. To send data to Kinesis, wrap the data (a `byte[]`) into a `Record` and call `IProducer.Send` and that's it!

The built-in implementation of `IProducer` can operate in either `Blocking` or `Background` modes:

<table>
	<tbody>
		<tr>
			<td><strong>Blocking</strong></td>
			<td><p>Data are saved into Kinesis straight away, and the task will complete as soon as the Kinesis request has completed.</p></td>
		</tr>
		<tr>
			<td><strong>Background</strong></td>
			<td><p>Data are saved into Kinesis by a number of background workers, the the task will complete as soon as it's submitted into a managed backlog.</p></td>
		</tr>
	</tbody>
</table>

At the time of writing, Amazon Kinesis has a **put latency of 103ms** which renders blocking sends rather undesirable in most use cases, hence by default, the built-in `IProducer` will operate in the `Background` mode.


#### Configuring the Producer

The built-in `IProducer` accepts a number of configuration options:

<table>
	<thead>
		<tr>
			<td><strong>Configuration</strong></td>
			<td>Default Value</td>
			<td>Description</td>
		</tr>
	</thead>
	<tbody>
		<tr>
			<td><strong>Mode</strong></td>
			<td><p>Background : </p>
				<ul>
					<li>HighWaterMarks = 1000</li>
					<li>HighWaterMarksMode = DropData</li>
				</ul>
			</td>
			<td>How to process the send requests.</td>
		</tr>
		<tr>
			<td><strong>LevelOfConcurrency</strong></td>
			<td>10</td>
			<td>The amount of concurrency for writing to the Kinesis.</td>
		</tr>
		<tr>
			<td><strong>MaxPutRecordAttempts</strong></td>
			<td>3</td>
			<td>Max number of attempts for putting a record into Kinesis.</td>
		</tr>
		<tr>
			<td><strong>ThrottleThreshold</strong></td>
			<td>
				<ul>
					<li>MaxThrottlePerMinute = 10</li>
					<li>ConsecutiveMinutes = 3</li>
				</ul>
			</td>
			<td>Threshold for the throttled calls before splitting shards.</td>
		</tr>
	</tbody>
</table>

To configure the producer, pass in an instance of `DarkseidConfig` when creating the producer using the static method `Producer.CreateNew`.


#### High water marks in the backlog

When operating in the `Background` mode, data is pushed to Kinesis in the background by a number of workers (the number is determined by the configured level of concurrency), if left unmanaged there is however a risk of the backlog growing indefinitely until we run out of memory.

To mitigate this risk, I borrowed the idea of **high water marks** from the brilliant [0MQ](http://zeromq.org/) library so that the backlog is allowed to only to a certain size before actions are taken to either:
* drop data
* block caller



## Features
