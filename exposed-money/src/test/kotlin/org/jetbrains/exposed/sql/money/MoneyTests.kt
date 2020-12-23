package org.jetbrains.exposed.sql.money

import org.javamoney.moneta.Money
import org.jetbrains.exposed.dao.EntityClass
import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.insertAndGetId
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.tests.DatabaseTestsBase
import org.jetbrains.exposed.sql.tests.TestDB
import org.jetbrains.exposed.sql.tests.shared.assertEquals
import org.junit.Ignore
import org.junit.Test
import java.math.BigDecimal
import javax.money.CurrencyUnit
import javax.money.MonetaryAmount

private const val AMOUNT_SCALE = 5

open class MoneyBaseTest : DatabaseTestsBase() {

    @Test
    fun testInsertSelectMoney() {
        testInsertedAndSelect(Money.of(BigDecimal.TEN, "USD"))
    }

    @Test
    fun testInsertSelectFloatingMoney() {
        testInsertedAndSelect(Money.of(BigDecimal("0.12345"), "USD"))
    }

    @Test
    @Ignore // TODO not supported yet
    fun testInsertSelectNull() {
        testInsertedAndSelect(null)
    }

    @Test(expected = ExposedSQLException::class)
    fun testInsertSelectOutOfLength() {
        testInsertedAndSelect(Money.of(BigDecimal.valueOf(12345678901), "CZK"))
    }

    @Test
    fun testSearchByCompositeColumn() {
        val money = Money.of(BigDecimal.TEN, "USD")

        withTables(Account) {
            Account.insertAndGetId {
                it[composite_money] = money
            }

            val predicates = listOf(
                    Account.composite_money eq money,
                    (Account.composite_money.currency eq money.currency),
                    (Account.composite_money.amount eq BigDecimal.TEN)
            )

            predicates.forEach {
                val found = AccountDao.find { it }

                assertEquals(1L, found.count())
                val next = found.iterator().next()
                assertEquals(money, next.money)
                assertEquals(money.currency, next.currency)
                assertEquals(BigDecimal.TEN.setScale(AMOUNT_SCALE), next.amount)
            }
        }
    }

    @Test
    fun testSearchByNullableCompositeColumn() {
        val money = null

        withTables(excludeSettings = listOf(TestDB.MYSQL, TestDB.H2_MYSQL, TestDB.POSTGRESQL, TestDB.POSTGRESQLNG), NullableAccount) {
            NullableAccount.insertAndGetId {
                it[composite_money] = money
            }

            val predicates = listOf(
                    NullableAccount.composite_money eq money,
                    (NullableAccount.composite_money.currency eq null),
                    (NullableAccount.composite_money.amount eq null)
            )

            predicates.forEach {
                val found = NullableAccountDao.find { it }

                assertEquals(1L, found.count())
                val next = found.iterator().next()
                assertEquals(money, next.money)
                assertEquals(null, next.currency)
                assertEquals(null, next.amount)
            }
        }
    }

    private fun testInsertedAndSelect(toInsert: Money?) {
        withTables(Account) {
            val accountID = Account.insertAndGetId {
                it[composite_money] = toInsert!!
            }

            val single = Account.slice(Account.composite_money).select { Account.id.eq(accountID) }.single()
            val inserted = single[Account.composite_money]

            assertEquals(toInsert, inserted)
        }

    }

}

class AccountDao(id: EntityID<Int>) : IntEntity(id) {

    val money : MonetaryAmount by Account.composite_money

    val currency : CurrencyUnit? by Account.composite_money.currency

    val amount : BigDecimal? by Account.composite_money.amount

    companion object : EntityClass<Int, AccountDao>(Account)

}

object Account : IntIdTable("AccountTable") {

    val composite_money = compositeMoney(8, AMOUNT_SCALE, "composite_money")

}

class NullableAccountDao(id: EntityID<Int>) : IntEntity(id) {

    val money : MonetaryAmount? by NullableAccount.composite_money

    val currency : CurrencyUnit? by NullableAccount.composite_money.currency

    val amount : BigDecimal? by NullableAccount.composite_money.amount

    companion object : EntityClass<Int, NullableAccountDao>(NullableAccount)

}

object NullableAccount : IntIdTable("AccountTable") {

    val composite_money = compositeMoney(8, AMOUNT_SCALE, "composite_money").nullable()

}
