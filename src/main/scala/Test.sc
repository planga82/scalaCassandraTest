import data.wastePerCountry

val x = new wastePerCountry("ES","2016","1000")
x.print()

def creaObjetoyAnade(year:String):Unit = {
  val x = new wastePerCountry("ES",year,"1000")
}


val bufferedSource = io.Source.fromFile("/home/pablo/EntornosTrabajo/wastePerCapitaScalaCassandra/data/cei_pc031.tsv")
var i = 1
var cabecera: Array[String]= Array()
var datos:List[wastePerCountry]= List()
for (line <- bufferedSource.getLines) {

  val cols = line.split("\t").map((x:String) => x.trim)
  if(i==1){
    cabecera = cols
  }else{
    for (j <- 1 to cols.length-1){
      val country:String = cols(0).split(",")(2)
      val year:String = cabecera(j)
      val waste:String = cols(j)
      datos = datos:+new wastePerCountry(country,year,waste)
    }
  }
  i = i+1

}
bufferedSource.close
for(x <- datos){
  x.print()
}

