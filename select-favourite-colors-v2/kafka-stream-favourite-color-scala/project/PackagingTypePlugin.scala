import sbt.{Def, _}

object PackagingTypePlugin extends AutoPlugin {
  override def buildSettings = {
    sys.props += "packagin.type" -> "jar"
    Nil
  }
}