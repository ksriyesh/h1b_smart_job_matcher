import os
import sys
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, LLM
from crewai_tools import SerperDevTool
from crewai.project import CrewBase, agent, crew
from crewai.process import Process
from .models import WebLookupOutput  # This must match your Pydantic model

# Setup paths and load environment
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

# Validate required keys
if not os.getenv("OPENAI_API_KEY") or not os.getenv("SERPER_API_KEY"):
    raise EnvironmentError("❌ Missing OPENAI_API_KEY or SERPER_API_KEY in .env")

@CrewBase
class H1BPipelineCrew:
    agents_config = "config/agents.yaml"
    tasks_config = "config/tasks.yaml"

    @agent
    def employer_web_processor(self) -> Agent:
        conf = self.agents_config["employer_web_processor"]
        return Agent(
            **conf,
            llm=LLM(model="gpt-4o-mini"),
            tools=[SerperDevTool(n=10)],
            allow_delegation=False,
            verbose=True
        )

    @crew
    def crew(self) -> Crew:
        web_lookup_task = Task(
            description=self.tasks_config["web_lookup_task"]["description"],
            agent=self.employer_web_processor(),
            expected_output=self.tasks_config["web_lookup_task"]["expected_output"],
            output_json=WebLookupOutput
        )

        return Crew(
            agents=[self.employer_web_processor()],
            tasks=[web_lookup_task],
            process=Process.sequential,
            verbose=True
        )

def run_agent_query(role: str, company: str) -> dict:
    """
    Executes the CrewAI job lookup task for a given role and company.
    
    Args:
        role (str): Target job role to search.
        company (str): Company name to look jobs for.

    Returns:
        dict: Parsed output from the CrewAI agent in the expected JSON schema.
    """
    crew_instance = H1BPipelineCrew().crew()
    result = crew_instance.run({"role": role, "company": company})
    return result
