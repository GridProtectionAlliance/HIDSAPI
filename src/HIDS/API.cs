//******************************************************************************************************
//  API.cs - Gbtc
//
//  Copyright © 2020, Grid Protection Alliance.  All Rights Reserved.
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
//  07/23/2020 - J. Ritchie Carroll
//       Generated original version of source code.
//
//******************************************************************************************************

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using InfluxDB.Client;
using InfluxDB.Client.Core;
using InfluxDB.Client.Core.Flux.Domain;
using Newtonsoft.Json;

namespace HIDS
{
    public partial class API : IDisposable
    {
        public static string DefaultOrganizationID { get; set; } = "gpa";

        private InfluxDBClient? m_client;
        private char[] m_token = Array.Empty<char>();
        private bool m_disposed;

        public string OrganizationID { get; set; } = DefaultOrganizationID;

        public string TokenID
        {
            get => new string(m_token);
            set => m_token = value.ToCharArray();
        }

        public async Task ConnectAsync(string influxDBHost)
        {
            InfluxDBClientOptions options = InfluxDBClientOptions.Builder.CreateNew()
                .Url(influxDBHost)
                .AuthenticateToken(m_token)
                .TimeOut(Timeout.InfiniteTimeSpan)
                .Build();

            InfluxDBClient client = InfluxDBClientFactory.Create(options);

            try
            {
                // Query the version to see
                // if the server is responding
                await client.VersionAsync();
                m_client = client;
            }
            catch
            {
                client.Dispose();
            }
        }

        public void Disconnect()
        {
            m_client?.Dispose();
            m_client = null;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (m_disposed)
                return;

            try
            {
                if (disposing)
                {
                    Disconnect();
                }
            }
            finally
            {
                m_disposed = true;
            }
        }

        private async IAsyncEnumerable<FluxRecord> ReadFluxRecordsAsync(string fluxQuery, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            if (m_client is null)
                throw new InvalidOperationException("Cannot read points: not connected to InfluxDB.");

            TaskCreationOptions taskCreationOptions = TaskCreationOptions.RunContinuationsAsynchronously;
            TaskCompletionSource<TaskCompletionSource<FluxRecord>> readyTaskSource = new TaskCompletionSource<TaskCompletionSource<FluxRecord>>(taskCreationOptions);

            void HandleRecord(ICancellable cancellable, FluxRecord record)
            {
                Task<TaskCompletionSource<FluxRecord>> readyTask = readyTaskSource.Task;
                TaskCompletionSource<FluxRecord> recordTaskSource = readyTask.GetAwaiter().GetResult();

                cancellationToken.ThrowIfCancellationRequested();

                // Order of events is important here; control cannot be returned
                // to the main loop until the new readyTaskSource is created
                readyTaskSource = new TaskCompletionSource<TaskCompletionSource<FluxRecord>>(taskCreationOptions);
                recordTaskSource.SetResult(record);
            }

            void HandleException(Exception ex)
            {
                Task<TaskCompletionSource<FluxRecord>> readyTask = readyTaskSource.Task;
                TaskCompletionSource<FluxRecord> recordTaskSource = readyTask.GetAwaiter().GetResult();
                recordTaskSource.SetException(ex);
            }

            void HandleComplete()
            {
                Task<TaskCompletionSource<FluxRecord>> readyTask = readyTaskSource.Task;
                TaskCompletionSource<FluxRecord> recordTaskSource = readyTask.GetAwaiter().GetResult();
                recordTaskSource.SetCanceled();
            }

            QueryApi queryAPI = m_client.GetQueryApi();

            // Do not await the query! HandleComplete will tell us when the query is finished
            _ = queryAPI.QueryAsync(fluxQuery, OrganizationID, HandleRecord, HandleException, HandleComplete);

            while (true)
            {
                TaskCompletionSource<FluxRecord> recordTaskSource = new TaskCompletionSource<FluxRecord>(taskCreationOptions);
                readyTaskSource.SetResult(recordTaskSource);

                FluxRecord record;
                try { record = await recordTaskSource.Task.ConfigureAwait(false); }
                catch (TaskCanceledException) { break; }

                yield return record;
            }
        }

        public static string FormatTimestamp(DateTime timestamp) =>
            JsonConvert.ToString(ForceUTC(timestamp)).Trim('"');

        // InfluxDB only accepts UTC timestamps,
        // and properly converting to UTC would change semantics for daily aggregation
        internal static DateTime ForceUTC(DateTime timestamp) =>
            DateTime.SpecifyKind(timestamp, DateTimeKind.Utc);

        // Time zone of timestamp read from InfluxDB cannot be determined
        internal static DateTime StripTimeZone(DateTime timestamp) =>
            DateTime.SpecifyKind(timestamp, DateTimeKind.Unspecified);
    }
}
