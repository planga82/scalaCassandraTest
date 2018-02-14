package scala.utils

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.{QueryBuilder => QB}
import me.eax.cassandra_example.utils._

import scala.collection.JavaConverters._
import scala.concurrent._

case class WPC_DTO(id: Int, country: String, year: String, waste: String)

case class TodoDAO(session: Session)(implicit ec: ExecutionContext) {

  private val table = "wastePerCountry"
  private val id = "id"
  private val country = "country"
  private val year = "year"
  private val waste = "waste"

  def createTable: Future[Unit] = {
    val query = s"create table if not exists $table ($id int primary key, $country text, $year text, $waste test )"
    session.executeAsync(query).map(_ => {})
  }

  def dropTable: Future[Unit] = {
    val query = s"drop table if exists $table"
    session.executeAsync(query).map(_ => {})
  }

  def insert(dto: WPC_DTO): Future[Unit] = {
    val query = {
      QB.insertInto(table)
        .value(id, dto.id)
        .value(country, dto.country)
        .value(year, dto.year)
        .value(waste, dto.waste)
    }
    session.executeAsync(query).map(_ => {})
  }

  def select: Future[Seq[WPC_DTO]] = {
    val query = {
      QB.select(id, description)
        .from(table)
    }

    for {
      resultSet <- session.executeAsync(query)
    } yield {
      resultSet
        .asScala
        .map(row => WPC_DTO(row.getInt(id), row.getString(country),row.getString(year),row.getString(waste)))
        .toSeq
    }
  }

  def delete(idToDelete: Long): Future[Unit] = {
    val query = {
      QB.delete().all()
        .from(table)
        .where(QB.eq(id, idToDelete))
    }
    session.executeAsync(query).map(_ => {})
  }
}
