Some notes on the output of the TFL API.

# Line statuses

You will get a list of dict items, each corresponding to a single line. (We will refer to each such dict as a "line dict".)

Each line dict contains a `modeName` entry which tells you what mode of transport the line relates to. The possible values are `bus`, `national-rail`, `tube`, `river-bus`, `dlr`, `cable-car`, `overground`, `tflrail`, `tram`, `elizabeth-line`. These are mostly self-explanatory, but a few points to note:

- TFL rail (`tflrail`) is the old name for the Elizabeth line (`elizabeth-line`). In the data, the last `tflrail` entry is 26 May 2022 and the first `elizabeth-line` entry is 17 May 2022. So it appears there is a week of overlap.
- `tube` excludes the DLR (`dlr`) and Elizabeth line (`tflrail`/`elizabeth-line`).

The line dict also has a `name` entry, telling you the line/route name.

Each line dict also has a `disruptions` entry, but as far as I can see this is always an empty list, even if there are actually disruptions on the line. So it is not useful.

The actual status of the line is observed by examining the `lineStatuses` entry in the line dict. This is a list of dicts each of which has information on the current status. There can be more than one status in effect at a time. The `statusSeverity` entry in the status dict is a numerical value indicating the level of severity of the status. The `statusSeverityDescription` is a string describing the status in words. 

A full outline of all possible severity levels can be found [here](https://api.tfl.gov.uk/Line/Meta/Severity). Values I have observed in the wild for `statusSeverityDescription` are:

| description     | modes                                                                      | meaning                                                                          |
|-----------------|----------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| Good Service    | all                                                                        | Service operating as normal.                                                     |
| Minor Delays    | national-rail, tube, dlr, overground, tflrail, tram, elizabeth-line        | Minor delays to service.                                                         |
| Severe Delays   | national-rail, tube, dlr, overground, tflrail, tram, elizabeth-line        | Severe delays to service.                                                        |
| Special Service | bus, national-rail, tube, river-bus, cable-car, overground, elizabeth-line | Some interruption or aberration in the service.                                  |
| Reduced Service | bus, tube, river-bus, dlr, overground, tflrail, tram, elizabeth-line       | Service is reduced. Could be planned or due to unforeseen circumstances.         |
| No Service      | national-rail, cable-car                                                   | Service is suspended for some unforeseen reason.                                 |
| Suspended       | tube, river-bus, dlr, overground, tflrail, tram, elizabeth-line            | Service is suspended for some unforeseen reason.                                 |
| Part Suspended  | tube, river-bus, dlr, overground, tflrail, tram, elizabeth-line            | Service is suspended on parts of the line for some unforeseen reason.            |
| Service Closed  | tube, dlr, overground, tflrail, tram, elizabeth-line                       | Service is closed as scheduled.                                                  |
| Planned Closure | tube, river-bus, dlr, cable-car, overground, tram, elizabeth-line          | Service is closed. Closure was planned in advance.                               |
| Part Closure    | tube, dlr, overground, tflrail, tram, elizabeth-line                       | Part of the line is closed. Partial closure was planned in advance.              |
| Bus Service     | bus                                                                        | Seems to be used for interruptions to bus service, similar to "Special Service". |

The `Special Service` status seems to be the main way to communicate disruptions to bus routes. For other modes, it is used only rarely, and rather inconsistently. For the tube, for example, it is sometimes used where service is disrupted due to strike action.

Statuses other than `Good Service` are typically accompanied by a `reason` entry (as a string) and a `disruption` entry (as a dict), which will give more information about the disruption to service. For National Rail lines, the reason is just a link to the National Rail website. The `disruption` will generally have a `category` entry which, it seems, is generally one of:

- `PlannedWork`: Disruption is due to some planned works.
- `RealTime`: Disruption is apparently unplanned.
- `Information`: This can either be information about a disruption or, when used in connection with a `Special Service` status, seems to just communicate some additional information about the service (ie, it doesn't necessarily indicate a disruption at all). For example, I have seen it used to advertise a TFL customer survey.

A line dict can contain multiple statuses. This could be the case if, for example, only part of a line is affected by a particular status (like if part of a line is delayed or closed). Statuses which are in effect for prolonged periods of time (like a line being partially closed for weeks for upgrade works, for example) may also appear even when the line is outside of normal service hours, so that a line could have both "Service Closed" and "Part Closure" statuses.
