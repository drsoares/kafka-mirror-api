        __ __      ______         __  ____                      ___    ____  ____
       / //_/___ _/ __/ /______ _/  |/  (_)_____________  _____/   |  / __ \/  _/
      / ,< / __ `/ /_/ //_/ __ `/ /|_/ / / ___/ ___/ __ \/ ___/ /| | / /_/ // /  
     / /| / /_/ / __/ ,< / /_/ / /  / / / /  / /  / /_/ / /  / ___ |/ ____// /   
    /_/ |_\__,_/_/ /_/|_|\__,_/_/  /_/_/_/  /_/   \____/_/  /_/  |_/_/   /___/   
                                                                             
A Java Api to enable mirroring topic(s) from one kafka broker to another.

### Motivation

This API was designed to solve a particular problem which was to create async replicas of a kafka topic in a different data center (DC).

Other kafka mirror solutions are mostly wrappers of the binary _kafka-mirror-maker.sh_ that is installed along with **kafka**, 
and launch a process, which makes difficult to embed it inside of an app, control when it starts and when it stops or to monitor it.

### Other Scenarios 

It can be used for other scenarios like active/active, in which data to be computed from one DC can be replicated to the other DC. 
However, if you go for active/active and you're writing for both DCs but just reading from one source, you may lose order.


### Requirements

* Java >= 8
* Maven
* Kafka >= 1.1.0

### Getting Started

Currently this artifact is not available in any public artifactory. So you need to clone and install it.

    git clone git@github.com:drsoares/kafka-mirror-api.git
    
    cd kafka-mirror-api/
    
    mvn clean install
    
Once it is avaiable on your .m2 repository, you can include it on your project, adding it to your classpath.

    Mirror mirror = new KafkaMirror(Collections.singleton("TopicToReplicate"), "source.bootstrap.servers:9092", "destination.bootstrap.servers:9092", new DefaultRecordTransformer());
    new Thread(() -> mirror.start()).start();
    
Once you write something for the topic ``TopicToReplicate`` this will consume the data from this topic and publish to a 
topic with the same name and same number of partitions on the other broker.

The `DefaultRecordTransformer` is responsible to convert a consumer record into a producer record, however it adds an header 
marking the record as _mirrored_, it keeps the partition value, key and value from the original source, ir order to ensure consistency.
It's also possible to change the destination topic, ex.:

    Map<String, String> topicMap = new HashMap<>();
    topicMap.put("source", "destination");
    
    RecordTransformer recordTransformer = new DefaultRecordTransformer(topicMap);
    
This will ensure that data written to _source_ topic will be written to _destination_ topic on the destination broker.
Just notice that the topology of the destination topic should be the same (same number of partitions).
Obviously you can create your own implementation of this RecordTransformer.

### Contributing

Please read CONTRIBUTING.md for details on our code of conduct, and the process for submitting pull requests to us.

### Author

+ Diogo Soares

### Contributors

+ James Cutajar (cutajarj)
+ Vitor Fernandes (balhau)

### License

This project is licensed under the Apache License - see the LICENSE.md file for details

