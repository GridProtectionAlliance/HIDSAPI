//******************************************************************************************************
//  API.Histograms.cs - Gbtc
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
using System.IO;
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
        public static string DefaultHistogramBucket { get; set; } = "histogram_bucket";

        public string HistogramBucket { get; set; } = DefaultHistogramBucket;

        public async Task WriteHistogramAsync(Histogram histogram)
        {
            if (m_client is null)
                throw new InvalidOperationException("Cannot write histograms: not connected to InfluxDB.");

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

            DateTime startTime = ForceUTC(histogram.Info.StartTime);
            DateTime endTime = ForceUTC(histogram.Info.EndTime);

            int sampleCount = histogram.Info.SamplingRate / histogram.Info.FundamentalFrequency;
            string cyclicHistogramData = ToString(histogram.Info.CyclicHistogramBins, sampleCount, histogram.CyclicHistogramData);
            string residualHistogramData = ToString(histogram.Info.ResidualHistogramBins, sampleCount, histogram.ResidualHistogramData);
            string frequencyHistogramData = ToString(histogram.Info.FrequencyHistogramBins, 0, histogram.FrequencyHistogramData);
            string rmsHistogramData = ToString(histogram.Info.RMSHistogramBins, 0, histogram.RMSHistogramData);

            PointData point = PointData
                .Measurement("histogram")
                .Tag("tag", histogram.Info.Tag)
                .Timestamp(startTime, WritePrecision.S)
                .Field("endTime", endTime.Ticks)
                .Field("cyclesMax", histogram.Info.CyclesMax)
                .Field("cyclesMin", histogram.Info.CyclesMin)
                .Field("residualMax", histogram.Info.ResidualMax)
                .Field("residualMin", histogram.Info.ResidualMin)
                .Field("frequencyMax", histogram.Info.FrequencyMax)
                .Field("frequencyMin", histogram.Info.FrequencyMin)
                .Field("rmsMax", histogram.Info.RMSMax)
                .Field("rmsMin", histogram.Info.RMSMin)
                .Field("cyclicHistogramBins", histogram.Info.CyclicHistogramBins)
                .Field("residualHistogramBins", histogram.Info.ResidualHistogramBins)
                .Field("frequencyHistogramBins", histogram.Info.FrequencyHistogramBins)
                .Field("rmsHistogramBins", histogram.Info.RMSHistogramBins)
                .Field("cyclicHistogram", cyclicHistogramData)
                .Field("residualHistogram", residualHistogramData)
                .Field("frequencyHistogram", frequencyHistogramData)
                .Field("rmsHistogram", rmsHistogramData);

            writeApi.WritePoint(HistogramBucket, OrganizationID, point);

            writeApi.Dispose();
            taskCompletionSource.TrySetResult(null);
            await taskCompletionSource.Task;
        }

        public IAsyncEnumerable<Histogram.Metadata> ReadHistogramMetadataAsync(IEnumerable<string> tags, DateTime startTime, CancellationToken cancellationToken = default) =>
            ReadHistogramMetadataAsync(tags, FormatTimestamp(startTime), cancellationToken: cancellationToken);

        public IAsyncEnumerable<Histogram.Metadata> ReadHistogramMetadataAsync(IEnumerable<string> tags, DateTime startTime, DateTime stopTime, CancellationToken cancellationToken = default) =>
            ReadHistogramMetadataAsync(tags, FormatTimestamp(startTime), FormatTimestamp(stopTime), cancellationToken: cancellationToken);

        public IAsyncEnumerable<Histogram.Metadata> ReadHistogramMetadataAsync(IEnumerable<string> tags, string start, string stop = "-0s", CancellationToken cancellationToken = default)
        {
            List<string> clauses = new List<string>(6) { };

            clauses.Add($"from(bucket: \"{HistogramBucket}\")");
            clauses.Add($"range(start: {start}, stop: {stop})");
            clauses.Add($"filter(fn: (r) => r._measurement == \"histogram\")");
            clauses.Add($"filter(fn: (r) => r._field !~ /^(cyclic|residual|frequency|rms)Histogram$/)");

            string tagExpression = string.Join("|", tags);

            if (tagExpression.Length > 0)
                clauses.Add($"filter(fn: (r) => r.tag =~ /{tagExpression}/)");

            clauses.Add("pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")");

            string fluxQuery = string
                .Join("\n  |> ", clauses)
                .Trim();

            return ReadHistogramMetadataAsync(fluxQuery, cancellationToken);
        }

        public Task<List<Histogram.Point>> ReadCyclicHistogramAsync(string tag, DateTime timestamp, CancellationToken cancellationToken = default) =>
            ReadHistogramAsync(tag, timestamp, "cyclicHistogram", cancellationToken);

        public Task<List<Histogram.Point>> ReadResidualHistogramAsync(string tag, DateTime timestamp, CancellationToken cancellationToken = default) =>
            ReadHistogramAsync(tag, timestamp, "residualHistogram", cancellationToken);

        public Task<List<Histogram.Point>> ReadFrequencyHistogramAsync(string tag, DateTime timestamp, CancellationToken cancellationToken = default) =>
            ReadHistogramAsync(tag, timestamp, "frequencyHistogram", cancellationToken);

        public Task<List<Histogram.Point>> ReadRMSHistogramAsync(string tag, DateTime timestamp, CancellationToken cancellationToken = default) =>
            ReadHistogramAsync(tag, timestamp, "rmsHistogram", cancellationToken);

        public IAsyncEnumerable<Histogram.Metadata> ReadHistogramMetadataAsync(string fluxQuery, CancellationToken cancellationToken = default)
        {
            IAsyncEnumerable<FluxRecord> fluxRecords = ReadFluxRecordsAsync(fluxQuery, cancellationToken);

            return fluxRecords.Select(record => new Histogram.Metadata()
            {
                Tag = record.GetValueByKey("tag")?.ToString(),
                FundamentalFrequency = Convert.ToInt32(record.GetValueByKey("fundamentalFrequency") ?? 0),
                SamplingRate = Convert.ToInt32(record.GetValueByKey("samplingRate") ?? 0),
                StartTime = StripTimeZone(record.GetTimeInDateTime().GetValueOrDefault()),
                EndTime = new DateTime(Convert.ToInt64(record.GetValueByKey("endTime") ?? 0L)),
                TotalCapturedCycles = Convert.ToInt32(record.GetValueByKey("totalCapturedCycles") ?? 0),
                CyclesMax = Convert.ToDouble(record.GetValueByKey("cyclesMax") ?? double.NaN),
                CyclesMin = Convert.ToDouble(record.GetValueByKey("cyclesMin") ?? double.NaN),
                ResidualMax = Convert.ToDouble(record.GetValueByKey("residualMax") ?? double.NaN),
                ResidualMin = Convert.ToDouble(record.GetValueByKey("residualMin") ?? double.NaN),
                FrequencyMax = Convert.ToDouble(record.GetValueByKey("frequencyMax") ?? double.NaN),
                FrequencyMin = Convert.ToDouble(record.GetValueByKey("frequencyMin") ?? double.NaN),
                RMSMax = Convert.ToDouble(record.GetValueByKey("rmsMax") ?? double.NaN),
                RMSMin = Convert.ToDouble(record.GetValueByKey("rmsMin") ?? double.NaN),
                CyclicHistogramBins = Convert.ToInt32(record.GetValueByKey("cyclicHistogramBins") ?? 0),
                ResidualHistogramBins = Convert.ToInt32(record.GetValueByKey("residualHistogramBins") ?? 0),
                FrequencyHistogramBins = Convert.ToInt32(record.GetValueByKey("frequencyHistogramBins") ?? 0),
                RMSHistogramBins = Convert.ToInt32(record.GetValueByKey("rmsHistogramBins") ?? 0)
            });
        }

        public async Task<List<Histogram.Point>> ReadHistogramAsync(string fluxQuery, CancellationToken cancellationToken = default)
        {
            IAsyncEnumerable<FluxRecord> fluxRecords = ReadFluxRecordsAsync(fluxQuery, cancellationToken);

            List<Histogram.Point> points = await fluxRecords
                .Select(record => record.GetValue())
                .Select(obj => obj?.ToString())
                .Where(histogramData => !string.IsNullOrEmpty(histogramData))
                .Select(FromString!)
                .FirstOrDefaultAsync(cancellationToken);

            return points ?? new List<Histogram.Point>(0);
        }

        private async Task<List<Histogram.Point>> ReadHistogramAsync(string tag, DateTime timestamp, string field, CancellationToken cancellationToken = default)
        {
            string formattedTimestamp = FormatTimestamp(timestamp);

            List<string> clauses = new List<string>(5)
            {
                $"from(bucket: \"{HistogramBucket}\")",
                $"range(start: {formattedTimestamp}, stop: date.add(d: 1ns, to: {formattedTimestamp}))",
                $"filter(fn: (r) => r._measurement == \"histogram\")",
                $"filter(fn: (r) => r.tag == \"{tag}\")",
                $"filter(fn: (r) => r._field == \"{field}\")"
            };

            string importSection = "import \"date\"";

            string querySection = string
                .Join("\n  |> ", clauses)
                .Trim();

            string fluxQuery = $"{importSection}\n{querySection}";
            return await ReadHistogramAsync(fluxQuery, cancellationToken);
        }

        private static List<Histogram.Point> FromString(string pointData)
        {
            byte[] bytes = Convert.FromBase64String(pointData);
            using MemoryStream memoryStream = new MemoryStream(bytes);
            using BinaryReader reader = new BinaryReader(memoryStream);

            Func<int> GetReadFunction(byte byteSize)
            {
                if (byteSize == 0)
                    return () => 0;
                if (byteSize == sizeof(byte))
                    return () => reader.ReadByte();
                if (byteSize == sizeof(ushort))
                    return () => reader.ReadUInt16();
                return () => reader.ReadInt32();
            }

            byte binByteSize = reader.ReadByte();
            byte sampleByteSize = reader.ReadByte();
            Func<int> binReadFunction = GetReadFunction(binByteSize);
            Func<int> sampleReadFunction = GetReadFunction(sampleByteSize);
            List<Histogram.Point> points = new List<Histogram.Point>();

            while (memoryStream.Length - memoryStream.Position > binByteSize + sampleByteSize + sizeof(float))
            {
                Histogram.Point point = new Histogram.Point();
                point.Bin = binReadFunction.Invoke();
                point.Sample = sampleReadFunction.Invoke();
                point.Value = reader.ReadSingle();
                points.Add(point);
            }

            return points;
        }

        private static string ToString(int bins, int samples, List<Histogram.Point> points)
        {
            using MemoryStream memoryStream = new MemoryStream();
            using BinaryWriter writer = new BinaryWriter(memoryStream);

            byte GetByteSize(int maxValue)
            {
                if (maxValue == 0)
                    return 0;
                if (maxValue < byte.MaxValue)
                    return sizeof(byte);
                if (maxValue < ushort.MaxValue)
                    return sizeof(ushort);
                return sizeof(int);
            }

            Action<int> GetWriteAction(int maxValue)
            {
                if (maxValue == 0)
                    return num => { };
                if (maxValue < byte.MaxValue)
                    return num => writer.Write((byte)num);
                if (maxValue < ushort.MaxValue)
                    return num => writer.Write((ushort)num);
                return num => writer.Write(num);
            }

            writer.Write(GetByteSize(bins));
            writer.Write(GetByteSize(samples));

            Action<int> binWriter = GetWriteAction(bins);
            Action<int> sampleWriter = GetWriteAction(samples);

            foreach (Histogram.Point point in points)
            {
                binWriter.Invoke(point.Bin);
                sampleWriter.Invoke(point.Sample);
                writer.Write(point.Value);
            }

            writer.Flush();

            byte[] bytes = memoryStream.ToArray();
            return Convert.ToBase64String(bytes);
        }
    }
}
