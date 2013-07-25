\name{set.user}
\alias{set.user}
\title{
  Switch user for the current process
}
\description{
  \code{set.user} attempts to change the user running the current
  process. Note that this is only possible if the process has teh
  privilege to do so.
}
\usage{
set.user(name, gid = NA)
}
\arguments{
  \item{name}{either a string specifying the username or an integer
    specifying the \code{uid}}    
  \item{gid}{optional, integer, desired \code{gid}}
}
\details{
  \code{set.user} can be used in two ways: with a user name or with a
  specific \code{uid}/\code{gid} combination. If used with a user name,
  then \code{set.user} attempts to find the user in the user database,
  changes the \code{uid}, \code{gid} and supplemental groups according
  to the user database.

  When \code{name} is a number, then \code{set.user} only sets the
  \code{uid} and (if \code{gid} is specified) \code{gid}. Supplemental
  groups are \emph{not} changed in that case.

  This function is typically used when R is started with elevated
  privileges (or is \code{suid}) and it needs to relinquish those to run
  as a particular user. The recommended way is to use the username and
  not the \code{uid}/\code{gid} specification.

  The function will raise an error if either of the required steps
  fails.
}
\value{
  integer, \code{uid} if used with a username, \code{TRUE} otherwise
}
%\references{
%}
\author{
  Simon Urbanek
}
\note{
  When changing users, make sure you also change the permissions for
  \code{tempdir()} \emph{before} you change the user. Otherwise R will
  not function properly as it cannot create temporary files.
}
%\seealso{
%}
\examples{
\dontrun{
set.user("myname")
}
}
\keyword{IO}
