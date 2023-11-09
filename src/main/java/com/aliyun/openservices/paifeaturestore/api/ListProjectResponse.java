package com.aliyun.openservices.paifeaturestore.api;

import com.aliyun.openservices.paifeaturestore.model.Project;

import java.util.List;

public class ListProjectResponse {
    List<Project> projects;

    public List<Project> getProjects() {
        return projects;
    }

    public void setProjects(List<Project> projects) {
        this.projects = projects;
    }

    @Override
    public String toString() {
        return "ListProjectResponse{" +
                "projects=" + projects +
                '}';
    }
}
