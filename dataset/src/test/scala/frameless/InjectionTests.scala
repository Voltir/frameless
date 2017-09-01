package frameless

import frameless.CollectTests.prop
import org.scalacheck._
import org.scalacheck.Prop._
import shapeless.test.illTyped

sealed trait Country
case object France extends Country
case object Russia extends Country

object Country {
  implicit val arbitrary: Arbitrary[Country] =
    Arbitrary(Arbitrary.arbitrary[Boolean].map(injection.invert))

  implicit val injection: Injection[Country, Boolean] =
    Injection(France.==, if (_) France else Russia)
}

sealed trait Food
case object Burger extends Food
case object Pasta extends Food
case object Rice extends Food

object Food {
  implicit val arbitrary: Arbitrary[Food] =
    Arbitrary(Arbitrary.arbitrary[Int].map(i => injection.invert(Math.abs(i % 3))))

  implicit val injection: Injection[Food, Int] =
    Injection(
      {
        case Burger => 0
        case Pasta => 1
        case Rice => 2
      },
      {
        case 0 => Burger
        case 1 => Pasta
        case 2 => Rice
      }
    )
}

// Supposingly coming from a java lib, shapeless can't derive stuff for this one :(
class LocalDateTime {
  var instant: Long = _

  override def equals(o: Any): Boolean =
    o.isInstanceOf[LocalDateTime] && o.asInstanceOf[LocalDateTime].instant == instant
}

object LocalDateTime {
  implicit val arbitrary: Arbitrary[LocalDateTime] =
    Arbitrary(Arbitrary.arbitrary[Long].map(injection.invert))

  implicit val injection: Injection[LocalDateTime, Long] =
    Injection(
      _.instant,
      long => { val ldt = new LocalDateTime; ldt.instant = long; ldt }
    )
}

class LocalDate {
  var days: Int = _

  override def equals(o: Any): Boolean =
    o.isInstanceOf[LocalDate] && o.asInstanceOf[LocalDate].days == days
}

object LocalDate {
  implicit val arbitrary: Arbitrary[LocalDate] =
    Arbitrary(Arbitrary.arbitrary[SQLDate].map(injection.invert))

  implicit val injection: Injection[LocalDate, SQLDate] =
    Injection(
      ld => SQLDate(ld.days),
      date => { val ld = new LocalDate; ld.days = date.days; ld }
    )
}

case class Person(age: Int, name: String)

object Person {
  val tupled = (Person.apply _).tupled

  implicit val arbitrary: Arbitrary[Person] =
    Arbitrary(Arbitrary.arbTuple2[Int, String].arbitrary.map(tupled))

  implicit val injection: Injection[Person, (Int, String)] =
    Injection(p => unapply(p).get, tupled)
}

case class Account(id: Int, modified: LocalDate)

case class I[A](value: A)

object I {
  implicit def injection[A]: Injection[I[A], A] = Injection(_.value, I(_))
  implicit def typedEncoder[A: TypedEncoder]: TypedEncoder[I[A]] = TypedEncoder.usingInjection[I[A], A]
  implicit def arbitrary[A: Arbitrary]: Arbitrary[I[A]] = Arbitrary(Arbitrary.arbitrary[A].map(I(_)))
}

object Nick extends org.scalatest.Tag("Nick")

class InjectionTests extends TypedDatasetSuite {
  test("Injection based encoders") {
    check(forAll(prop[Country] _))
    check(forAll(prop[LocalDateTime] _))
    check(forAll(prop[Food] _))
    check(forAll(prop[X1[Country]] _))
    check(forAll(prop[X1[LocalDateTime]] _))
    check(forAll(prop[X1[Food]] _))
    check(forAll(prop[X1[X1[Country]]] _))
    check(forAll(prop[X1[X1[LocalDateTime]]] _))
    check(forAll(prop[X1[X1[Food]]] _))
    check(forAll(prop[X2[Country, X2[LocalDateTime, Food]]] _))
    check(forAll(prop[X3[Country, LocalDateTime, Food]] _))

    check(forAll(prop[I[Int]] _))
    check(forAll(prop[I[Option[Int]]] _))
    check(forAll(prop[I[I[Int]]] _))
    check(forAll(prop[I[I[Option[Int]]]] _))

    check(forAll(prop[I[X1[Int]]] _))
    check(forAll(prop[I[I[X1[Int]]]] _))
    check(forAll(prop[I[I[Option[X1[Int]]]]] _))

    check(forAll(prop[Option[I[Int]]] _))
    check(forAll(prop[Option[I[X1[Int]]]] _))

    assert(TypedEncoder[I[Int]].targetDataType == TypedEncoder[Int].targetDataType)
    assert(TypedEncoder[I[I[Int]]].targetDataType == TypedEncoder[Int].targetDataType)

    assert(TypedEncoder[I[Option[Int]]].nullable)
  }

  test("TypedEncoder[Person] is ambiguous") {
    illTyped("implicitly[TypedEncoder[Person]]", "ambiguous implicit values.*")
  }

  test("Resolve ambiguity by importing usingInjection") {
    import TypedEncoder.usingInjection

    check(forAll(prop[X1[Person]] _))
    check(forAll(prop[X1[X1[Person]]] _))
    check(forAll(prop[X2[Person, Person]] _))
    check(forAll(prop[Person] _))

    assert(TypedEncoder[Person].targetDataType == TypedEncoder[(Int, String)].targetDataType)
  }

  test("Resolve ambiguity by importing usingDerivation") {
    import TypedEncoder.usingDerivation
    assert(implicitly[TypedEncoder[Person]].isInstanceOf[RecordEncoder[Person, _]])
    check(forAll(prop[Person] _))
  }

  test("Expected schema", Nick) {
    //import frameless.syntax._
    import TypedEncoder.usingInjection
    frameless.TypedExpressionEncoder.targetStructType(TypedEncoder[Account]).printTreeString()
    println("!!!!!!!!!!!!!!!!!!!!!!")
    println(TypedExpressionEncoder(TypedEncoder[Account]).schema)
    val ld = new LocalDate()
    println("???????????????????????????//")
    ld.days = 1
    val wurt: org.apache.spark.sql.Dataset[Account] =
      session.createDataset[Account](List(Account(1,ld)))(TypedExpressionEncoder(TypedEncoder[Account]))
    println("ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ")
    wurt.show()
    println(wurt.schema)
  }
}
