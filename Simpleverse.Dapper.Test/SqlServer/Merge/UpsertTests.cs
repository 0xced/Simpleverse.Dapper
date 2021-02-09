using Xunit;
using System.Linq;
using System.Threading.Tasks;
using Dapper.Contrib.Extensions;
using Simpleverse.Dapper.SqlServer.Merge;

namespace Simpleverse.Dapper.Test.SqlServer.Merge
{
	[Collection("SqlServerCollection")]
	public class UpsertTests : IClassFixture<DatabaseFixture>
	{
		DatabaseFixture fixture;

		public UpsertTests(DatabaseFixture fixture)
		{
			this.fixture = fixture;
		}

		[Fact]
		public async Task UpsertAsyncTest()
		{
			using (var connection = fixture.GetConnection())
			{
				// arrange
				connection.Open();
				await connection.DeleteAllAsync<ExplicitKey>();
				var record = TestData.ExplicitKeyData(1).Single();
				connection.Insert(record);
				record.Name = "Updated";

				// act
				await connection.UpsertAsync(record);

				// assert
				var updatedRecord = connection.Get<ExplicitKey>(1);
				Assert.NotNull(updatedRecord);
				Assert.Equal("Updated", updatedRecord.Name);
				Assert.Equal(record.Name, updatedRecord.Name);
			}
		}

		[Fact]
		public async Task UpsertBulkAsyncTest()
		{
			using (var connection = fixture.GetConnection())
			{
				// arrange
				connection.Open();
				await connection.DeleteAllAsync<ExplicitKey>();
				var records = TestData.ExplicitKeyData(10);
				var inserted = connection.Insert(records);
				records = records.Skip(1);
				foreach(var record in records)
				{
					record.Name = (record.Id + 2).ToString();
				}

				// act
				var updated = await connection.UpsertBulkAsync(records);

				// assert
				var updatedRecords = connection.GetAll<ExplicitKey>();
				Assert.Equal(9, updated);
				Assert.Equal("1", updatedRecords.First(x => x.Id == 1).Name);
				for (int i = 0; i < records.Count(); i++)
				{
					var record = records.ElementAt(i);
					var updatedRecord = updatedRecords.FirstOrDefault(x => x.Id == record.Id);
					Assert.NotNull(updatedRecord);
					Assert.Equal(record.Name, updatedRecord.Name);
				}
			}
		}
	}
}
