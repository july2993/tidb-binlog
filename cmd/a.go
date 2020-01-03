package main

import (
	"github.com/pingcap/tidb-binlog/tests/dailytest"
	"github.com/pingcap/tidb-binlog/tests/util"
)

var TableSQLs = []string{`
create table ptest(
	a int primary key,
	b double NOT NULL DEFAULT 2.0,
	c varchar(10) NOT NULL,
	d time unique
);
`,
	`
create table itest(
	a int,
	b double NOT NULL DEFAULT 2.0,
	c varchar(10) NOT NULL,
	d time unique,
	PRIMARY KEY(a, b)
);
`,
	`
create table ntest(
	a int,
	b double NOT NULL DEFAULT 2.0,
	c varchar(10) NOT NULL,
	d time unique
);
`}

func main() {
	db, err := util.CreateSourceDB()
	if err != nil {
		panic(err)
	}

	dailytest.RunDailyTest(db, TableSQLs, 10, 1000, 10)
}
