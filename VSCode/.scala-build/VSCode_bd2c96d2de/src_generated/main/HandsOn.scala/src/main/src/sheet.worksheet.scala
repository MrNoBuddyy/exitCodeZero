
package HandsOn$u002Escala.src.main.src



final class sheet$u002Eworksheet$_ {
def args = sheet$u002Eworksheet_sc.args$
def scriptPath = """HandsOn.scala/src/main/src/sheet.worksheet.sc"""
/*<script>*/
val v=1

/*</script>*/ /*<generated>*/
/*</generated>*/
}

object sheet$u002Eworksheet_sc {
  private var args$opt0 = Option.empty[Array[String]]
  def args$set(args: Array[String]): Unit = {
    args$opt0 = Some(args)
  }
  def args$opt: Option[Array[String]] = args$opt0
  def args$: Array[String] = args$opt.getOrElse {
    sys.error("No arguments passed to this script")
  }

  lazy val script = new sheet$u002Eworksheet$_

  def main(args: Array[String]): Unit = {
    args$set(args)
    val _ = script.hashCode() // hashCode to clear scalac warning about pure expression in statement position
  }
}

export sheet$u002Eworksheet_sc.script as sheet$u002Eworksheet

