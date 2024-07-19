//******************************************************************************************************
//  PointStream.cs - Gbtc
//
//  Copyright © 2022, Grid Protection Alliance.  All Rights Reserved.
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
//  06/01/2022 - Stephen C. Wills
//       Generated original version of source code.
//
//******************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace HIDS
{
    public class PointStream : Stream
    {
        private class State : IDisposable
        {
            public API HIDS { get; }
            public IAsyncEnumerator<object> Enumerator { get; }
            public CancellationTokenSource CancellationTokenSource { get; }
            private bool IsDisposed { get; set; }

            public State(API hids, Action<IQueryBuilder> configureQuery, bool pointCountQuery)
            {
                CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
                CancellationToken cancellationToken = cancellationTokenSource.Token;

                IAsyncEnumerable<object> enumerable = pointCountQuery
                    ? hids.ReadPointCountAsync(configureQuery, cancellationToken).Cast<object>()
                    : hids.ReadPointsAsync(configureQuery, cancellationToken).Cast<object>();

                HIDS = hids;
                Enumerator = enumerable.GetAsyncEnumerator(cancellationToken);
                CancellationTokenSource = cancellationTokenSource;
            }

            public State(API hids, Func<API, CancellationToken, IAsyncEnumerable<object>> queryFunc)
            {
                CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
                CancellationToken cancellationToken = cancellationTokenSource.Token;

                IAsyncEnumerable<object> enumerable = queryFunc(hids, cancellationToken);

                HIDS = hids;
                Enumerator = enumerable.GetAsyncEnumerator(cancellationToken);
                CancellationTokenSource = cancellationTokenSource;
            }

            public void Dispose()
            {
                if (IsDisposed)
                    return;

                try
                {
                    CancellationTokenSource.Cancel();

                    Enumerator
                        .DisposeAsync()
                        .AsTask()
                        .GetAwaiter()
                        .GetResult();

                    HIDS.Dispose();
                    CancellationTokenSource.Dispose();
                }
                finally
                {
                    IsDisposed = true;
                }
            }
        }

        private delegate Task<int> AsyncBufferHandler(byte[] buffer, int offset, int count, CancellationToken cancellationToken);

        private int m_position;

        private Lazy<Task<State>> LazyState { get; }
        private Lazy<AsyncBufferHandler> LazyBufferHandler { get; }
        private bool IsDisposed { get; set; }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;

        public override long Length =>
            throw new NotSupportedException();

        public override long Position
        {
            get => m_position;
            set => throw new NotSupportedException();
        }

        private PointStream(Func<Task<API>> hidsFactory, Action<IQueryBuilder> configureQuery, bool pointCountQuery)
        {
            async Task<State> CreateStateAsync()
            {
                API hids = await hidsFactory();
                return new State(hids, configureQuery, pointCountQuery);
            }

            LazyState = new Lazy<Task<State>>(CreateStateAsync);
            LazyBufferHandler = new Lazy<AsyncBufferHandler>(CreateBufferHandler);
        }

        private PointStream(Func<Task<API>> hidsFactory, Func<API, CancellationToken, IAsyncEnumerable<object>> queryFunc)
        {
            async Task<State> CreateStateAsync()
            {
                API hids = await hidsFactory();
                return new State(hids, queryFunc);
            }

            LazyState = new Lazy<Task<State>>(CreateStateAsync);
            LazyBufferHandler = new Lazy<AsyncBufferHandler>(CreateBufferHandler);
        }

        public override void Flush() =>
            ThrowIfDisposed();

        public override int Read(byte[] buffer, int offset, int count) =>
            ReadAsync(buffer, offset, count).GetAwaiter().GetResult();

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            await LazyBufferHandler.Value(buffer, offset, count, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="PointStream"/> object and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            if (IsDisposed)
                return;

            try
            {
                if (disposing && LazyState.IsValueCreated)
                    LazyState.Value.Dispose();
            }
            finally
            {
                IsDisposed = true;          // Prevent duplicate dispose.
                base.Dispose(disposing);    // Call base class Dispose().
            }
        }

        private AsyncBufferHandler CreateBufferHandler()
        {
            ThrowIfDisposed();

            Task<State> stateTask = LazyState.Value;
            Encoding encoding = new UTF8Encoding(false);
            Encoder encoder = encoding.GetEncoder();

            char[] chars = Array.Empty<char>();
            int charOffset = 0;
            bool isComplete = false;

            return async (buffer, offset, count, cancellationToken) =>
            {
                int totalBytes = 0;

                if (isComplete)
                    return totalBytes;

                while (true)
                {
                    if (charOffset >= chars.Length)
                    {
                        State state = await stateTask;
                        IAsyncEnumerator<object> enumerator = state.Enumerator;

                        if (!await enumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                            break;

                        object obj = enumerator.Current;

                        chars = JObject
                            .FromObject(obj)
                            .ToString(Formatting.None)
                            .Append('\n')
                            .ToArray();

                        charOffset = 0;
                    }

                    int charCount = chars.Length - charOffset;

                    try
                    {
                        encoder.Convert(
                            chars, charOffset, charCount,
                            buffer, offset, count, flush: false,
                            out int charsUsed, out int bytesUsed, out bool completed);

                        charOffset += charsUsed;
                        totalBytes += bytesUsed;
                        offset += bytesUsed;
                        count -= bytesUsed;
                        m_position += bytesUsed;

                        if (!completed)
                            return totalBytes;
                    }
                    catch (ArgumentException)
                    {
                        // If there were never enough bytes in the buffer for encoding,
                        // the exception must be thrown to differentiate this from EOS
                        if (totalBytes == 0)
                            throw;

                        // If this occurs during a partial read because there are
                        // not enough bytes left in the buffer after reading the last
                        // part, we can safely return the number of bytes that were read
                        return totalBytes;
                    }
                }

                try
                {
                    int charCount = chars.Length - charOffset;

                    encoder.Convert(
                        chars, charOffset, charCount,
                        buffer, offset, count, flush: true,
                        out int charsUsed, out int bytesUsed, out bool completed);

                    totalBytes += bytesUsed;
                    m_position += bytesUsed;
                    isComplete = completed;
                }
                catch (ArgumentException)
                {
                    // If there were never enough bytes in the buffer for encoding,
                    // the exception must be thrown to differentiate this from EOS
                    if (totalBytes == 0)
                        throw;
                }

                return totalBytes;
            };
        }

        private void ThrowIfDisposed()
        {
            if (IsDisposed)
                throw new ObjectDisposedException(nameof(PointStream));
        }

        public static PointStream QueryPoints(Func<API> hidsFactory, Action<IQueryBuilder> configureQuery) =>
            new PointStream(() => Task.FromResult(hidsFactory()), configureQuery, false);

        public static PointStream QueryPointCount(Func<API> hidsFactory, Action<IQueryBuilder> configureQuery) =>
            new PointStream(() => Task.FromResult(hidsFactory()), configureQuery, true);

        public static PointStream QueryPoints(Func<Task<API>> hidsFactory, Action<IQueryBuilder> configureQuery) =>
            new PointStream(hidsFactory, configureQuery, false);

        public static PointStream QueryPointCount(Func<Task<API>> hidsFactory, Action<IQueryBuilder> configureQuery) =>
            new PointStream(hidsFactory, configureQuery, true);

        public static PointStream QueryPoints(Func<API> hidsFactory, Func<API, CancellationToken, IAsyncEnumerable<Point>> queryFunc) =>
            new PointStream(() => Task.FromResult(hidsFactory()), queryFunc);

        public static PointStream QueryPointCount(Func<API> hidsFactory, Func<API, CancellationToken, IAsyncEnumerable<PointCount>> queryFunc) =>
            new PointStream(() => Task.FromResult(hidsFactory()), queryFunc);
        public static PointStream QueryPoints(Func<Task<API>> hidsFactory, Func<API, CancellationToken, IAsyncEnumerable<Point>> queryFunc) =>
            new PointStream(hidsFactory, queryFunc);

        public static PointStream QueryPointCount(Func<Task<API>> hidsFactory, Func<API, CancellationToken, IAsyncEnumerable<PointCount>> queryFunc) =>
            new PointStream(hidsFactory, queryFunc);
    }
}
