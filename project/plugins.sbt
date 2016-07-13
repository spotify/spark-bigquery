resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.2")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
addSbtPlugin("org.scalastyle" % "scalastyle-sbt-plugin" % "0.8.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.3.5")
addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.4")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.1")
