// Copyright (c) 2021 mobus sunsc0220@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

var (
	Branch    = "main"
	Author    = "mobus"
	Email     = "<sv0202@163.com>"
	Date      = "2021-12-29"
	Commit    = "821288f"
	GoVersion = "go1.17.2 linux/amd64"
)

var version = &cli.Command{
	Name:    "version",
	Aliases: []string{"v"},
	Usage:   "show version info",
	Action: func(c *cli.Context) error {
		fmt.Println("branch: ", Branch)
		fmt.Println("author: ", Author)
		fmt.Println("email: ", Email)
		fmt.Println("date: ", Date)
		fmt.Println("git commit: ", Commit)
		fmt.Println(GoVersion)
		return nil
	},
}
