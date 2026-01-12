# Contributing to goframe

First off, thank you for considering contributing to `goframe`! It’s people like you who make this library better for the Go community.

## 1. How to Contribute

### Claiming an Issue
To avoid multiple people working on the same task simultaneously:
1.  Find an open issue you are interested in.
2.  Comment on the issue: "I would like to work on this."
3.  Wait for a maintainer to assign the issue to you before you start coding.
4.  If you have questions about the implementation, ask them in the issue thread so everyone can benefit from the discussion.

### Pull Requests
* **Draft PRs**: If your work is in progress, please open a "Draft PR" so others know it is being handled.
* **Branching**: Create a new branch for every feature or bug fix.
* **Single Focus**: Each PR should address only one issue or feature.

---

## 2. Standards and Expectations

### AI-Generated Code Policy
We welcome the use of AI tools (like GitHub Copilot or ChatGPT) to assist your workflow, but with strict conditions:
* **Ownership**: You are responsible for every line of code you submit. You must be able to explain the logic of your code during the review process if required.
* **Verification**: Do not "blindly" copy and paste. Ensure the code respects our internal structures, such as `DataFrame` and `Column[any]`.
* **Context Awareness**: Ensure generated code handles Go's type safety and generics correctly.

---

## 3. Development Workflow

### Testing
All contributions must include unit tests. 
* Place tests in the `goframe_tests/` directory.
* Ensure all tests pass by running:
    ```bash
    go test -v ./...
    ```
* Our GitHub Actions CI will automatically run these tests on every push and pull request.

### Coding Style
* Run `go fmt ./...` before committing.
* Follow standard Go idioms (e.g., return errors instead of panicking, use descriptive variable names).
* Use the "Suggested Changes" feature on GitHub to collaborate during the review process.

---

## 4. Reporting Issues

### Bugs
Please use our [Bug Report Template](.github/ISSUE_TEMPLATE/bug_report.md) and include:
* Steps to reproduce.
* Expected vs. actual behavior.
* A minimal code snippet demonstrating the issue.

### Feature Requests
Please use our [Feature Request Template](.github/ISSUE_TEMPLATE/feature_request.md) to describe the problem the feature would solve and your proposed solution.

---

**Thank you for helping us build a better data framework for Go!**