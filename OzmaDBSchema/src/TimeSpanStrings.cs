using System;
using System.Text;
using System.Globalization;
using NodaTime;

// Extracted from `NpgsqlTimeSpan` source code.
namespace OzmaDBSchema.Npgsql
{
    public static class TimeSpanStrings
    {
        public static Period Parse(string str)
        {
            if (str == null)
            {
                throw new ArgumentNullException(nameof(str));
            }

            try
            {
                str = str.Trim();
                if (str.Length == 0)
                {
                    return Period.Zero;
                }

                var years = 0;
                var months = 0;
                var days = 0;
                decimal totalTicks = 0m;
                var tokens = str.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);

                for (var i = 0; i < tokens.Length;)
                {
                    var token = tokens[i];

                    if ((token == "+" || token == "-") && i + 1 < tokens.Length && tokens[i + 1].IndexOf(':') >= 0)
                    {
                        token += tokens[i + 1];
                        i += 2;
                    }
                    else
                    {
                        i += 1;
                    }

                    if (TryParseClockTime(token, out var clockTicks))
                    {
                        totalTicks += clockTicks;
                        continue;
                    }

                    if (!decimal.TryParse(token, NumberStyles.Number, CultureInfo.InvariantCulture, out var amount))
                    {
                        throw new FormatException();
                    }

                    // Keep legacy behavior: a single number means hours.
                    if (i >= tokens.Length)
                    {
                        totalTicks += amount * TimeSpan.TicksPerHour;
                        break;
                    }

                    var unit = tokens[i].Trim().TrimEnd(',').ToLowerInvariant();
                    i += 1;
                    ApplyUnit(amount, unit, ref years, ref months, ref days, ref totalTicks);
                }

                var builder = new PeriodBuilder
                {
                    Years = years,
                    Months = months,
                    Days = days,
                    Ticks = (long)totalTicks
                };
                return builder.Build();
            }
            catch (OverflowException)
            {
                throw;
            }
            catch (Exception)
            {
                throw new FormatException();
            }
        }

        private static bool IsInteger(decimal value)
        {
            return decimal.Truncate(value) == value;
        }

        private static bool TryParseClockTime(string value, out decimal ticks)
        {
            ticks = 0m;
            if (value.IndexOf(':') < 0)
            {
                return false;
            }

            var isNegative = value[0] == '-';
            var parts = value.Split(':');
            int hours;
            int minutes = 0;
            decimal seconds = 0m;

            switch (parts.Length)
            {
                case 2:
                    hours = int.Parse(parts[0], CultureInfo.InvariantCulture);
                    minutes = int.Parse(parts[1], CultureInfo.InvariantCulture);
                    break;
                case 3:
                    hours = int.Parse(parts[0], CultureInfo.InvariantCulture);
                    minutes = int.Parse(parts[1], CultureInfo.InvariantCulture);
                    seconds = decimal.Parse(parts[2], CultureInfo.InvariantCulture);
                    break;
                default:
                    throw new FormatException();
            }

            if (isNegative)
            {
                minutes *= -1;
                seconds *= -1;
            }

            ticks = hours * TimeSpan.TicksPerHour + minutes * TimeSpan.TicksPerMinute + seconds * TimeSpan.TicksPerSecond;
            return true;
        }

        private static void ApplyUnit(decimal amount, string unit, ref int years, ref int months, ref int days, ref decimal ticks)
        {
            if (unit.StartsWith("year", StringComparison.Ordinal) || unit == "yr" || unit == "y")
            {
                if (!IsInteger(amount))
                {
                    throw new FormatException();
                }

                years += (int)amount;
            }
            else if (unit.StartsWith("mon", StringComparison.Ordinal) || unit == "month")
            {
                if (!IsInteger(amount))
                {
                    throw new FormatException();
                }

                months += (int)amount;
            }
            else if (unit.StartsWith("day", StringComparison.Ordinal) || unit == "d")
            {
                if (!IsInteger(amount))
                {
                    throw new FormatException();
                }

                days += (int)amount;
            }
            else if (unit.StartsWith("hour", StringComparison.Ordinal) || unit == "hr" || unit == "h")
            {
                ticks += amount * TimeSpan.TicksPerHour;
            }
            else if (unit.StartsWith("min", StringComparison.Ordinal) || unit == "m")
            {
                ticks += amount * TimeSpan.TicksPerMinute;
            }
            else if (unit.StartsWith("sec", StringComparison.Ordinal) || unit == "s")
            {
                ticks += amount * TimeSpan.TicksPerSecond;
            }
            else
            {
                throw new FormatException();
            }
        }

        public static bool TryParse(string str, out Period result)
        {
            try
            {
                result = Parse(str);
                return true;
            }
            catch (Exception)
            {
                result = Period.Zero;
                return false;
            }
        }

        public static string ToString(Period period)
        {
            var sb = new StringBuilder();
            bool isNegative = period.Months < 0;
            if (period.Months != 0)
            {
                sb.Append(period.Months).Append(Math.Abs(period.Months) == 1 ? " mon " : " mons ");
            }
            if (period.Days != 0)
            {
                if (period.Months < 0 && period.Days > 0)
                {
                    sb.Append('+');
                }
                sb.Append(period.Days).Append(Math.Abs(period.Days) == 1 ? " day " : " days ");
            }
            if (period.HasTimeComponent || sb.Length == 0)
            {
                var totalTicks =
                    period.Hours * TimeSpan.TicksPerHour +
                    period.Minutes * TimeSpan.TicksPerMinute +
                    period.Seconds * TimeSpan.TicksPerSecond +
                    period.Milliseconds * TimeSpan.TicksPerMillisecond +
                    period.Ticks +
                    period.Nanoseconds / 100L;
                if (totalTicks < 0)
                {
                    sb.Append('-');
                }
                else if (period.Days < 0 || (period.Days == 0 && period.Months < 0))
                {
                    sb.Append('+');
                }
                totalTicks = Math.Abs(totalTicks);
                // calculate total seconds and then subtract total whole minutes in seconds to get just the seconds and fractional part
                var totalSeconds = totalTicks / TimeSpan.TicksPerSecond;
                var totalMinutes = totalSeconds / 60;
                var totalHours = totalMinutes / 60;
                var minutes = totalMinutes % 60;
                var seconds = totalSeconds % 60;
                var microseconds = totalTicks % TimeSpan.TicksPerSecond / 10;
                sb.Append(totalHours.ToString("D2")).Append(':').Append(minutes.ToString("D2")).Append(':').Append(seconds.ToString("D2")).Append('.').Append(microseconds.ToString("D6"));
            }
            return sb.ToString();
        }
    }
}
