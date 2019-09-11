package com.ilife.analyzer.serializer;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Stuff {
	String title;
	String summary;
	String tagging;
	String url;
	String type;
	String category;
	String source;
	List<String> images;
	List<Map<String,String>> props;
	Company seller;
	Company distributor;
	Company producer;
	Link link;
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getSummary() {
		return summary;
	}
	public void setSummary(String summary) {
		this.summary = summary;
	}
	public String getTagging() {
		return tagging;
	}
	public void setTagging(String tagging) {
		this.tagging = tagging;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public List<String> getImages() {
		return images;
	}
	public void setImages(List<String> images) {
		this.images = images;
	}
	public List<Map<String,String>> getProps() {
		return props;
	}
	public void setProps(List<Map<String,String>> props) {
		this.props = props;
	}
	public Company getSeller() {
		return seller;
	}
	public void setSeller(Company seller) {
		this.seller = seller;
	}
	public Company getDistributor() {
		return distributor;
	}
	public void setDistributor(Company distributor) {
		this.distributor = distributor;
	}
	public Company getProducer() {
		return producer;
	}
	public void setProducer(Company producer) {
		this.producer = producer;
	}
	public Link getLink() {
		return link;
	}
	public void setLink(Link link) {
		this.link = link;
	}
}
