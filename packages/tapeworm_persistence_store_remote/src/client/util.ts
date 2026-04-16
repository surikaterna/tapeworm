import _ from "lodash";
import type { ICommit } from "tapeworm";

type CommitWithAuthorative = ICommit & { authorative?: boolean };

export function getLastAuthorizedCommitSequence(
  commits: CommitWithAuthorative[],
): number {
  var commit = _.findLast(commits, function (commit) {
    return commit.authorative !== undefined ? commit.authorative : false;
  });

  if (!commit) {
    return -1;
  } else {
    return commit.commitSequence;
  }
}

export function getLastCommitSequence(
  commits: CommitWithAuthorative[] | null,
): number {
  if (!commits || commits.length === 0) {
    return -1;
  } else {
    return commits[commits.length - 1].commitSequence;
  }
}

export function commitsMatch(commit1: ICommit, commit2: ICommit): boolean {
  //commits "identical"
  //	if(commit1.id === commit2.id) {
  if (commit1.commitSequence === commit2.commitSequence) {
    if (commit1.events.length === commit2.events.length) {
      for (var i = 0; i < commit1.events.length; i++) {
        var e1 = commit1.events[i];
        var e2 = commit2.events[i];
        return e1.type === e2.type && e1.correlationId === e2.correlationId;
      }
    }
  }
  //	} //else if ()
  return false;
}
