﻿//******************************************************************************************************
//  IQueryBuilder.cs - Gbtc
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
//  11/12/2020 - Stephen C. Wills
//       Generated original version of source code.
//
//******************************************************************************************************

using System;
using System.Collections.Generic;

namespace HIDS
{
    public interface IQueryBuilder
    {
        IQueryBuilder Range(DateTime startTime);
        IQueryBuilder Range(DateTime startTime, DateTime stopTime);
        IQueryBuilder Range(string start);
        IQueryBuilder Range(string start, string stop);
        IQueryBuilder RangeFilters(IEnumerable<Tuple<DateTime, DateTime>> ranges);
        IQueryBuilder FilterTags(params string[] includedTags);
        IQueryBuilder FilterTags(IEnumerable<string> includedTags);
        IQueryBuilder FilterTime(params TimeFilter[] filters);
        IQueryBuilder FilterTime(IEnumerable<TimeFilter> filters);
        IQueryBuilder TestQuality(uint invalidFlags);
        IQueryBuilder Aggregate(string duration);
        IEnumerable<Tuple<DateTime, DateTime>> GetTimeRanges();
        string BuildPointQuery();
        string BuildCountQuery();
    }
}
