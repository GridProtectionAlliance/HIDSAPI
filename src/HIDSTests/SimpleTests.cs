using HIDS;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using System.Threading.Tasks;

namespace HIDSTests
{
    [TestClass]
    public class SimpleTests
    {
        private static string TestURL { get; } = "http://vmhidsdev:8086/";
        private static string TestTag { get; } = "GPA.TestDevice.TestTrend";
        private static string TestBucket { get; } = "test_bucket";
        private static string TestOrganization { get; } = "gpa";
        private static string TestToken { get; } = "tJwORIjY-qdtZ13K9xBmBqEYfEczSb_GYuKLO1vVqS7o11nHtZ5aQDT87pgSFO6SYkBxh9nadtBVfvcIrkNoeQ==";

        public TestContext TestContext { get; set; }

        [TestMethod]
        public void WriteTest()
        {
            async Task WriteTestAsync()
            {
                using API hids = await ConnectAsync();

                TestContext.WriteLine("Starting write...");
                await hids.WritePointsAsync(new[] { new Point { Tag = TestTag, Minimum = 1.0D, Maximum = 10.0D, Average = 5.0D, QualityFlags = 0u, Timestamp = DateTime.UtcNow } });
                TestContext.WriteLine("Write complete.");
            }

            Task writeTask = WriteTestAsync();
            writeTask.GetAwaiter().GetResult();
        }

        [TestMethod]
        public void ReadTest()
        {
            async Task ReadTestAsync()
            {
                using API hids = await ConnectAsync();

                TestContext.WriteLine("Starting read...");

                await foreach (Point point in hids.ReadPointsAsync(BuildQuery))
                    TestContext.WriteLine($"Point = {point.Tag} with Max = {point.Maximum}, Min = {point.Minimum}, Avg = {point.Average} @ {point.Timestamp}");

                TestContext.WriteLine("Read complete.");
            }

            Task readTask = ReadTestAsync();
            readTask.GetAwaiter().GetResult();
        }

        [TestMethod]
        public void StreamTest()
        {
            TestContext.WriteLine("Starting stream...");

            using (PointStream stream = PointStream.QueryPoints(ConnectAsync, BuildQuery))
            using (StreamReader reader = new StreamReader(stream))
            {
                while (true)
                {
                    string line = reader.ReadLine();

                    if (line is null)
                        break;

                    TestContext.WriteLine(line);
                }
            }

            TestContext.WriteLine("Stream complete.");
        }

        private async Task<API> ConnectAsync()
        {
            API hids = new API();
            hids.PointBucket = TestBucket;
            hids.OrganizationID = TestOrganization;
            hids.TokenID = TestToken;

            TestContext.WriteLine("Connecting...");
            await hids.ConnectAsync(TestURL);
            TestContext.WriteLine("Connected.");

            return hids;
        }

        private void BuildQuery(IQueryBuilder builder) => builder
            .Range("-48h")
            .FilterTags(new[] { TestTag });
    }
}
