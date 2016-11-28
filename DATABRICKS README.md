Databricks Readme
==============

To be able to use this connector with Databricks there are a few things you need to do

First and most important is that the Google BQ client expects an Env variable with the location of the json key file, so ensure your Databricks cluster has the file saved with all new clusters

You can add that via a init scipt, save it inside the /databricks/init/ folder

Once you save the file locally use the databricks [api](https://docs.databricks.com/release-notes/product/2.30.html#set-environmental-variables-in-spark-from-rest-api) to launch a cluster with an env variable set up
You can use this curl command to do that 

```
%sh curl -H "Content-Type: application/json" -X 
POST \ -u username:passord url-of-databricks-instance:443/api/2.0/clusters/create \
 -d 
 ' { "cluster_name" : "spark 161-hadoop-1", 
 "spark_version": "1.6.1-ubuntu15.10-hadoop1", 
 "num_workers" : 1, 
 "spark_conf" : { "spark.speculation" : true }, 
 "spark_env_vars" : {"GOOGLE_APPLICATION_CREDENTIALS" :"/databricks/Justeat-platform-events-c750aee2059a.json"} 
 }'

```

Once that is done you need to shade your dependencies because databricks has been set up with avro 1.4 and this connector uses spark avro 1.8, also there are Guava dependency conflicts that need resolving

You can use the [maven shade plugin](https://maven.apache.org/plugins/maven-shade-plugin/) or the [sbt](https://github.com/sbt/sbt-assembly) 

Below is an example build.sbt file



```
name := "SampleInDatabricks"

version := "1.0"
scalaVersion := "2.11.8"
assemblyJarName := "uber-SampleInDatabricks-1.0-SNAPSHOT.jar"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided",
  "joda-time" % "joda-time" % "2.9.6"
)

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

```
If you use SBT ensure that the connector jar is in the lib folder, the shade plugin will package everything in the lib folder into the uber jar

Or if Maven is your preference, below is an example of the maven pom

```
<plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>2.3</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals>
              <goal>shade</goal>
            </goals>
          </execution>
        </executions>
        <configuration>
          <minimizeJar>true</minimizeJar>
          <shadedArtifactAttached>true</shadedArtifactAttached>
          <shadedClassifierName>fat</shadedClassifierName>
          <relocations>
            <relocation>
              <pattern>com.google</pattern>
              <shadedPattern>shaded.guava</shadedPattern>
              <includes>
                <include>com.google.**</include>
              </includes>
          <excludes>
            <exclude>com.google.common.base.Optional</exclude>
            <exclude>com.google.common.base.Absent</exclude>
            <exclude>com.google.common.base.Present</exclude>
          </excludes>
        </relocation>
      </relocations>
      <filters>
        <filter>
          <artifact>*:*</artifact>
          <excludes>
            <exclude>META-INF/*.SF</exclude>
            <exclude>META-INF/*.DSA</exclude>
            <exclude>META-INF/*.RSA</exclude>
          </excludes>
        </filter>
      </filters>
      <finalName>uber-${project.artifactId}-${project.version}</finalName>
    </configuration>
  </plugin>

  ```


