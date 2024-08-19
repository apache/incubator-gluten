/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

function detectISSUESID(title) {
  if (!title) {
    return null;
  }
  const matched = /^\[GLUTEN-\d+\]/.exec(title);
  if (!matched) {
    return null;
  }
  const issues_number = matched[0].replace(/[^0-9]/ig,"");
  return issues_number;
}

async function haveComment(github, context, pullRequestNumber, body) {
  const options = {
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: pullRequestNumber,
    page: 1
  };
  while (true) {
    const response = await github.rest.issues.listComments(options);
    if (response.data.some(comment => comment.body === body)) {
      return true;
    }
    if (!/;\s*rel="next"/.test(response.headers.link || "")) {
      break;
    }
    options.page++;
  }
  return false;
}

async function commentISSUESURL(github, context, pullRequestNumber, issuesID) {
  const issuesURL = `https://github.com/apache/incubator-gluten/issues/${issuesID}`;
  if (await haveComment(github, context, pullRequestNumber, issuesURL)) {
    return;
  }
  await github.rest.issues.createComment({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: pullRequestNumber,
    body: issuesURL
  });
}

module.exports = async ({github, context}) => {
  const pullRequestNumber = context.payload.number;
  const title = context.payload.pull_request.title;
  const issuesID = detectISSUESID(title);
  if (issuesID) {
    await commentISSUESURL(github, context, pullRequestNumber, issuesID);
  }
};
