//******************************************************************************************************
//  API.Points.cs - Gbtc
//
//  Copyright © 2023, Grid Protection Alliance.  All Rights Reserved.
//
//  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
//  the NOTICE file distributed with this work for additional information regarding copyright ownership.
//  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may not use this
//  file except in compliance with the License. You may obtain a copy of the License at:
//
//      http://opensource.org/licenses/MIT
//
//  Unless agreed to in writing, the subject software distributed under the License is distributed on an
//  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
//  License for the specific language governing permissions and limitations.
//
//  Code Modification History:
//  ----------------------------------------------------------------------------------------------------
//  09/26/2023 - Stephen C. Wills
//       Generated original version of source code.
//
//******************************************************************************************************

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Core.Flux.Domain;
using InfluxDB.Client.Writes;

namespace HIDS
{
    public partial class API
    {
        public static string DefaultPointBucket { get; set; } = "point_bucket";

        public string PointBucket { get; set; } = DefaultPointBucket;

        public async Task WritePointsAsync(IEnumerable<Point> points)
        {
            if (m_client is null)
                throw new InvalidOperationException("Cannot write points: not connected to InfluxDB.");

            TaskCreationOptions runContinuationsAsynchronously = TaskCreationOptions.RunContinuationsAsynchronously;
            TaskCompletionSource<object?> taskCompletionSource = new TaskCompletionSource<object?>(runContinuationsAsynchronously);

            using WriteApi writeApi = m_client.GetWriteApi();

            writeApi.EventHandler += (sender, args) =>
            {
                if (args is WriteErrorEvent errorEvent)
                    taskCompletionSource.TrySetException(errorEvent.Exception);

                if (args is WriteRuntimeExceptionEvent exceptionEvent)
                    taskCompletionSource.TrySetException(exceptionEvent.Exception);
            };

            foreach (Point point in points)
            {
                if (taskCompletionSource.Task.IsFaulted)
                    break;

                writeApi.WriteMeasurement(PointBucket, OrganizationID, WritePrecision.Ns, point);
            }

            writeApi.Dispose();
            taskCompletionSource.TrySetResult(null);
            await taskCompletionSource.Task;
        }

        public IAsyncEnumerable<Point> ReadPointsAsync(IEnumerable<string> tags, DateTime startTime, DateTime stopTime, CancellationToken cancellationToken = default) =>
            ReadPointsAsync(tags, FormatTimestamp(startTime), FormatTimestamp(stopTime), cancellationToken);

        public IAsyncEnumerable<Point> ReadPointsAsync(IEnumerable<string> tags, DateTime startTime, CancellationToken cancellationToken = default) =>
            ReadPointsAsync(tags, FormatTimestamp(startTime), cancellationToken: cancellationToken);

        public IAsyncEnumerable<Point> ReadPointsAsync(IEnumerable<string> tags, string start, string stop = "-0s", CancellationToken cancellationToken = default)
        {
            void ConfigureQuery(IQueryBuilder queryBuilder) => queryBuilder
                .Range(start, stop)
                .FilterTags(tags)
                .TestQuality(~0u);

            return ReadPointsAsync(ConfigureQuery, cancellationToken);
        }

        public IAsyncEnumerable<Point> ReadPointsAsync(Action<IQueryBuilder> configureQuery, CancellationToken cancellationToken = default)
        {
            QueryBuilder queryBuilder = new QueryBuilder(PointBucket);
            configureQuery(queryBuilder);

            string query = queryBuilder.BuildPointQuery();
            return ReadPointsAsync(query, cancellationToken);
        }

        public IAsyncEnumerable<PointCount> ReadPointCountAsync(Action<IQueryBuilder> configureQuery, CancellationToken cancellationToken = default)
        {
            QueryBuilder queryBuilder = new QueryBuilder(PointBucket);
            configureQuery(queryBuilder);

            string query = queryBuilder.BuildCountQuery();
            return ReadPointCountAsync(query, cancellationToken);
        }

        public IAsyncEnumerable<Point> ReadPointsAsync(string fluxQuery, CancellationToken cancellationToken = default)
        {
            IAsyncEnumerable<FluxRecord> fluxRecords = ReadFluxRecordsAsync(fluxQuery, cancellationToken);

            return fluxRecords.Select(record => new Point()
            {
                Tag = record.GetValueByKey("tag")?.ToString(),
                Timestamp = StripTimeZone(record.GetTimeInDateTime().GetValueOrDefault()),
                QualityFlags = Convert.ToUInt32(record.GetValueByKey("flags") ?? 0),
                Minimum = Convert.ToDouble(record.GetValueByKey("min") ?? double.NaN),
                Maximum = Convert.ToDouble(record.GetValueByKey("max") ?? double.NaN),
                Average = Convert.ToDouble(record.GetValueByKey("avg") ?? double.NaN)
            });
        }

        public IAsyncEnumerable<PointCount> ReadPointCountAsync(string fluxQuery, CancellationToken cancellationToken = default)
        {
            IAsyncEnumerable<FluxRecord> fluxRecords = ReadFluxRecordsAsync(fluxQuery, cancellationToken);

            return fluxRecords.Select(record => new PointCount()
            {
                Tag = record.GetValueByKey("tag")?.ToString(),
                Timestamp = StripTimeZone(record.GetTimeInDateTime().GetValueOrDefault()),
                Count = Convert.ToUInt64(record.GetValue())
            });
        }
    }
}
