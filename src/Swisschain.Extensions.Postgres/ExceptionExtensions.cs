using System;
using Npgsql;

namespace Swisschain.Extensions.Postgres
{
    public static class ExceptionExtensions
    {
        public static bool IsPrimaryKeyViolationException(this PostgresException e)
        {
            const string constraintViolationErrorCode = "23505";
            const string primaryKeyNamePrefix = "pk_";
            const string primaryKeyNameSuffix = "_pkey";

            return string.Equals(e.SqlState, constraintViolationErrorCode, StringComparison.InvariantCultureIgnoreCase) &&
                   (e.ConstraintName.StartsWith(primaryKeyNamePrefix, StringComparison.InvariantCultureIgnoreCase) ||
                    e.ConstraintName.EndsWith(primaryKeyNameSuffix, StringComparison.InvariantCultureIgnoreCase));
        }

        public static bool IsUniqueConstraintViolationException(this PostgresException e, string constraintName)
        {
            const string constraintViolationErrorCode = "23505";

            return string.Equals(e.SqlState, constraintViolationErrorCode, StringComparison.InvariantCultureIgnoreCase) &&
                   string.Equals(e.ConstraintName, constraintName, StringComparison.InvariantCultureIgnoreCase);
        }
    }
}
