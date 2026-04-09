export interface NavLink {
  label: string;
  href: string;
}

export interface ContentLink {
  title: string;
  href: string;
  target?: '_blank' | '_self';
}

export interface TournamentSummary {
  id: string;
  name: string;
  place?: string;
  startDate?: string;
  endDate?: string;
  links?: {
    rules?: string;
    news?: string;
    players?: string;
    rounds?: string;
    rankings?: string;
    games?: string;
  };
}

export interface LinkCategory {
  title: string;
  href?: string;
  items: ContentLink[];
}

export interface PlayerCategory {
  title: string;
  players: ContentLink[];
}

export interface FeaturedTab {
  key: string;
  label: string;
  sections: LinkCategory[];
}
