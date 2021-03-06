﻿using Xunit;
using System.Linq;
using Dapper.Contrib.Extensions;
using Simpleverse.Dapper.SqlServer;

namespace Simpleverse.Dapper.Test.SqlServer
{
	[Collection("SqlServerCollection")]
	public class DeleteTests : IClassFixture<DatabaseFixture>
	{
		DatabaseFixture fixture;

		public DeleteTests(DatabaseFixture fixture)
		{
			this.fixture = fixture;
		}

		[Fact]
		public void DeleteAsyncTest()
		{
			using (var connection = fixture.GetConnection())
			{
				// arange
				connection.Open();
				connection.DeleteAll<ExplicitKey>();
				var records = TestData.ExplicitKeyData(10);
				var inserted = connection.Insert(records);
				records = records.Skip(2).Take(5);

				// act
				var deleted = connection.DeleteBulkAsync(records).Result;

				// assert
				Assert.Equal(5, deleted);
				var remaningRecords = connection.GetAll<ExplicitKey>();
				Assert.Equal(5, remaningRecords.Count());
				foreach(var record in records)
				{
					var remaningRecord = remaningRecords.FirstOrDefault(x => x.Id == record.Id);
					Assert.Null(remaningRecord);
				}
			}
		}
	}
}
