//******************************************************************************************************
//  CyclicHistogram.cs - Gbtc
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
//  09/22/2023 - Stephen C. Wills
//       Generated original version of source code.
//
//******************************************************************************************************

using System;
using System.Collections.Generic;

namespace HIDS
{
    public class Histogram
    {
        public class Metadata
        {
            public string? Tag { get; set; }
            public int FundamentalFrequency { get; set; }
            public int SamplingRate { get; set; }
            public DateTime StartTime { get; set; }
            public DateTime EndTime { get; set; }
            public int TotalCapturedCycles { get; set; }
            public double CyclesMax { get; set; }
            public double CyclesMin { get; set; }
            public double ResidualMax { get; set; }
            public double ResidualMin { get; set; }
            public double FrequencyMax { get; set; }
            public double FrequencyMin { get; set; }
            public double RMSMax { get; set; }
            public double RMSMin { get; set; }
            public int CyclicHistogramBins { get; set; }
            public int ResidualHistogramBins { get; set; }
            public int FrequencyHistogramBins { get; set; }
            public int RMSHistogramBins { get; set; }
        }

        public class Point
        {
            public int Bin { get; set; }
            public int Sample { get; set; }
            public float Value { get; set; }
        }

        public Metadata Info { get; } = new Metadata();
        public List<Point> CyclicHistogramData { get; set; } = new List<Point>();
        public List<Point> ResidualHistogramData { get; set; } = new List<Point>();
        public List<Point> FrequencyHistogramData { get; set; } = new List<Point>();
        public List<Point> RMSHistogramData { get; set; } = new List<Point>();
    }
}
